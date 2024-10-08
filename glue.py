from dataclasses import dataclass
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime
from pyspark.pandas import spark
from pyspark.pandas.frame import DataFrame
from pyspark.sql.functions import collect_list, count, current_timestamp, explode, lit, when, window, size


from pyspark.sql.functions import from_json, col, size
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
import boto3
import json


@dataclass
class DynamoParams:
    id: str
    window_begin: datetime
    window_end: datetime
    accumulate: int
    is_over_threshold:bool
    create_timestamp: datetime

class GluePythonSampleTest:

    def __init__(self):
        params = []
        self.credentials = {}
        if '--JOB_NAME' in sys.argv:
            params.append('JOB_NAME')
            self.args = getResolvedOptions(sys.argv, params)
            self.infer_field = "`$json$data_infer_schema$_temporary$`"
            self.checkpoint = self.args["TempDir"] + "/" + self.args["JOB_NAME"] + "/checkpoint/"
            
        else:
            self.args = getResolvedOptions(sys.argv, params)
            import logging
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.DEBUG) 
            console_handler.setFormatter(formatter)
            self.logger = logging.getLogger()
            self.logger.addHandler(console_handler)
            self.infer_field = "`$json$data_infer_schema$.temporary$`"
            self.checkpoint = "/home/glue_user/ck"
            session = boto3.Session(profile_name='AppDev')  # Replace 'myprofile' with your profile name
            c = session.get_credentials()
            self.credentials = {
                "awsAccessKeyId": c.access_key,
                "awsSecretKey": c.secret_key,
            }
            
        self.context = GlueContext(SparkContext())
        self.logger = self.context.get_logger()
        self.spark = self.context.spark_session
        self.spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", False)
        self.spark.conf.set("spark.streaming.backpressure.enabled", True)
        self.job = Job(self.context)
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table('pipeline_circuit_breaker_state_glue') 


        if 'JOB_NAME' in self.args:
            jobname = self.args['JOB_NAME']
        else:
            jobname = "test"
        self.job.init(jobname, self.args)

    def invoke_lambda(self, batch_df, batch_id):
        # Initialize the Lambda client
        batch_json = batch_df.collect()
        # Invoke Lambda for each record in the batch
        self.logger.info(f"collection complete.................................................................")
        for record in batch_json:
            self.logger.info(f"get record.................................................................: {record}")
            content = {
                'type': 'aws glue', 
                'begin': record[0][0].isoformat(),
                'end': record[0][1].isoformat(),
                'count': record[2]
            }
            self.logger.info(f"get content.................................................................: {content}")
            self.logger.info(f"trying to invoke lambda.................................................................")
            response = self.lambda_client.invoke(
                FunctionName='StreamingTest',  # Replace with your Lambda function name
                InvocationType='Event',  # Asynchronous invocation
                Payload=json.dumps(content)
            )
            self.logger.info(f"Lambda invoked with response: {response}")

    def check_db_theshold(self, batch_df, batch_id):

        try:
            new_timestamp =datetime.now()
            self.logger.info(f'get incomming data....................')
            incomming= batch_df.withColumn('accumulate', size(col("device_token_list")))
            self.logger.info(f'get data success....................')
            row = incomming.collect()   
            # row = incomming.first()
            if len(row) == 0:
                self.logger.info(f'incomming data empty....................')
            else:
                time_window = row[0]['window']
                total_count = row[0]['accumulate']
                db_threshold = 100
                self.logger.info(f'reocrds less then db theshold: {db_threshold}, count: {total_count}....................')
                self.sync_to_dynamo([
                    DynamoParams(
                        id= 'db',
                        window_begin= time_window[0],
                        window_end= time_window[1],
                        accumulate= total_count,
                        is_over_threshold= total_count > db_threshold,
                        create_timestamp= new_timestamp
                    ) ]
                )
                self.logger.info(f'theshold_check success....................')
        except Exception as ex:
            self.logger.error(f'theshold_check fail....................')
            self.logger.error(ex)
            
    def check_device_theshold(self, batch_df, batch_id):

        try:
            new_timestamp =datetime.now()
            device_threshold = 10
            df = batch_df.filter(col("accumulate") > device_threshold ).collect()   
            self.sync_to_dynamo(
            [ 
                DynamoParams(
                    id= f'device#{r["device_token"]}',
                    window_begin= r['window'][0],
                    window_end=  r['window'][0],
                    accumulate= r["accumulate"],
                    is_over_threshold= r["accumulate"] > device_threshold,
                    create_timestamp= new_timestamp
                ) for r in df                   
            ])
            self.logger.info(f'theshold_check success....................')
        except Exception as ex:
            self.logger.error(f'theshold_check fail....................')
            self.logger.error(ex)

    def sync_to_dynamo(self, params: list[DynamoParams]):

        for p in params:
            try:
                response = self.table.update_item(
                    Key={
                        'id': p.id
                    },
                UpdateExpression="set window_begin = :window_begin,  winddow_end = :window_end, is_over_threshold = :flag, accumulate = :cnt, create_timestamp = :stamp",
                ConditionExpression="attribute_not_exists(create_timestamp) OR create_timestamp < :stamp",  
                ExpressionAttributeValues={
                    ':window_begin': str(int(p.window_begin.timestamp())),
                    ':window_end': str(int(p.window_end.timestamp())),
                    ':cnt':  p.accumulate,
                    ':flag': p.is_over_threshold,            
                    ':stamp': str(int(p.create_timestamp.timestamp()))
                },
            )
                self.logger.info('write_to_dynamodb success............................................................')
                self.logger.info(json.dumps(response))
            except Exception as e:
                self.logger.error(f"Failed to fetch or update the record: {e}")


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
                    "maxFetchTimeInMs": 15000,
                    "inferSchema": "true",
                    "addRecordTimestamp": "true"
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
        self.logger.info('print infer schema............................................................')
        self.logger.info(self.infer_field)

        json_schema = StructType([
            StructField("device_token", StringType(), True),
            StructField("data", ArrayType(StringType()), True)
        ])

        parsed_data = dataframe_AmazonKinesis_node1726723872587.withColumn(
            "parsed_json", from_json(col(self.infer_field), json_schema)
        )

        self.logger.info('print parsed schema............................................................')
        self.logger.info( str(parsed_data.schema))
        
        # Extract the `data` field from the parsed JSON
        extracted_data = parsed_data.select(col("parsed_json.device_token"), col("parsed_json.data"))

        self.logger.info('print extracted_data schema............................................................')
        self.logger.info( str(extracted_data.schema))
        
        # Count the elements in the `data` field
        data_with_count = extracted_data.withColumn("data_count", size(col("data")))

        self.logger.info('print data_with_count schema............................................................')
        self.logger.info( str(data_with_count.schema))
        

        data_with_timestamp  = data_with_count.withColumn("processing_time", current_timestamp())

        self.logger.info('print data_with_timestamp schema............................................................')
        self.logger.info( str(data_with_timestamp.schema))
        

                
        tumbling_windowed_db = data_with_timestamp \
            .withWatermark("processing_time", "5 minutes") \
            .groupBy(
                window(data_with_timestamp["processing_time"], "5 minutes")
            ) \
            .agg(collect_list(col("device_token")) \
            .alias("device_token_list"))

        tumbling_windowed_device = data_with_timestamp \
            .withWatermark("processing_time", "5 minutes") \
            .groupBy(
                window(data_with_timestamp["processing_time"], "5 minutes"),
                col("device_token")
            ) \
            .agg(count("*").alias("accumulate"))

        query1 = tumbling_windowed_db.writeStream \
            .outputMode("update") \
            .trigger(processingTime= '15 seconds')\
            .option("checkpointLocation", self.checkpoint + '/db') \
            .foreachBatch(self.check_db_theshold) \
            .start()
        

        query2 = tumbling_windowed_device.writeStream \
            .outputMode("update") \
            .trigger(processingTime= '15 seconds')\
            .option("checkpointLocation", self.checkpoint + '/device') \
            .foreachBatch(self.check_device_theshold) \
            .start()\
            .awaitTermination()


if __name__ == '__main__':
    GluePythonSampleTest().run()
