import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, when, window, size


from pyspark.sql.functions import from_json, col, size
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import boto3
import json



class GluePythonSampleTest:

    def __init__(self):
        params = []
        self.credentials = {}
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
            self.infer_field = "`$json$data_infer_schema$_temporary$`"
            self.checkpoint = self.args["TempDir"] + "/" + self.args["JOB_NAME"] + "/checkpoint/"
            
        else:
            import logging
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG) 
            console_handler.setFormatter(formatter)
            self.logger = logging.getLogger()
            self.logger.addHandler(console_handler)
            self.infer_field = "`$json$data_infer_schema$.temporary$`"
            self.checkpoint = "/tmp/checkpoint"
            session = boto3.Session(profile_name='AppDev')  # Replace 'myprofile' with your profile name
            c = session.get_credentials()
            self.credentials = {
                "awsAccessKeyId": c.access_key,
                "awsSecretKey": c.secret_key,
            }
            
        self.args = getResolvedOptions(sys.argv, params)
        self.context = GlueContext(SparkContext())
        self.logger = self.context.get_logger()
        self.spark = self.context.spark_session
        self.spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
        self.job = Job(self.context)
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('pipeline_circuit_breaker_state') 


        if 'JOB_NAME' in self.args:
            jobname = self.args['JOB_NAME']
        else:
            jobname = "test"
        self.job.init(jobname, self.args)

    def invoke_lambda(self, batch_df, batch_id):
        # Initialize the Lambda client
        batch_json = batch_df.collect()
        # Invoke Lambda for each record in the batch
        self.logger.info(f"collection comlete.................................................................")
        for record in batch_json:
            self.logger.info(f"get record.................................................................: {record}")
            content = {
                'type': 'aws glue', 
                'begin': record[0][0].isoformat(),
                'end': record[0][1].isoformat(),
                'count': record[1]
            }
            self.logger.info(f"get content.................................................................: {content}")
            self.logger.info(f"trying to invoke lambda.................................................................")
            response = self.lambda_client.invoke(
                FunctionName='StreamingTest',  # Replace with your Lambda function name
                InvocationType='Event',  # Asynchronous invocation
                Payload=json.dumps(content)
            )
            self.logger.info(f"Lambda invoked with response: {response}")

    def write_to_dynamodb(self, batch_df, batch_id):
        # Collect the data from the DataFrame
        batch_json = batch_df.collect()
        new_timestamp = int(datetime.now().timestamp())
        # Insert each record into DynamoDB
        for record in batch_json:
            token = record[1]
            is_over_threshold = int(record[2]) > 10
            try:
                response = self.table.update_item(
                    Key={
                        'device_token': token
                    },
                UpdateExpression="set is_over_threshold = :flag, create_timestamp = :stamp",
                ConditionExpression="attribute_not_exists(create_timestamp) OR create_timestamp < :stamp",  # Only update if the new timestamp is more recent
                ExpressionAttributeValues={
                    ':flag': is_over_threshold,            
                    ':stamp': str(new_timestamp)   
                },
            )

            except Exception as e:
                print(f"Failed to fetch or update the record: {e}")


    def run(self):
        # init_time= (datetime.datetime.now() - datetime.timedelta(seconds=60)).isoformat()
        options =  {
                **{
                    "typeOfData": "kinesis", 
                    "streamARN": "arn:aws:kinesis:us-east-1:719709800508:stream/yuan-playground", 
                    "classification": "json", 
                    "startingPosition": "LATEST", 
                    # "startingPosition": f"\'{init_time}\'",
                    # "startingPosition": "2023-04-04T08:00:00Z", 
                    # "startingPosition": init_time, 
                    "inferSchema": "true"
                }, 
                **self.credentials}
             
        dataframe_AmazonKinesis_node1726723872587 = self.context.create_data_frame.from_options(
            connection_type="kinesis",
            connection_options= options,
            transformation_ctx="dataframe_AmazonKinesis_node1726723872587")
                        
        t = str(type(dataframe_AmazonKinesis_node1726723872587))
        s = str(dataframe_AmazonKinesis_node1726723872587.schema)
        self.logger.info('print type............................................................')
        self.logger.info(t)
        self.logger.info('print schema............................................................')
        self.logger.info(s)
  
        
        json_schema = StructType([
            StructField("device_token", StringType(), True),
            StructField("data", ArrayType(StringType()), True)
        ])

        parsed_data = dataframe_AmazonKinesis_node1726723872587.withColumn(
            "parsed_json", from_json(col(self.infer_field), json_schema)
             
        )
        
        # Extract the `data` field from the parsed JSON
        extracted_data = parsed_data.select(col("parsed_json.device_token"), col("parsed_json.data"))
        
        # Count the elements in the `data` field
        data_with_count = extracted_data.withColumn("data_count", size(col("data")))

        data_with_timestamp  = data_with_count.withColumn("processing_time", current_timestamp())
        
        tumbling_windowed_data = data_with_timestamp.groupBy(
            window(data_with_timestamp["processing_time"], "5 minutes"),
            data_with_timestamp["device_token"]  # Include device_token in the groupBy
        ).agg({"data_count": "sum"}).withColumnRenamed("sum(data_count)", "total_data_count")
        
        query = tumbling_windowed_data.writeStream \
            .outputMode("complete") \
            .trigger(processingTime= '10 seconds')\
            .option("checkpointLocation", self.checkpoint) \
            .foreachBatch(self.write_to_dynamodb) \
            .start()


        query.awaitTermination()
        

if __name__ == '__main__':
    GluePythonSampleTest().run()
