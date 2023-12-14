try:

    import os
    import sys
    import uuid
    import json
    import yaml
    from azure.storage.blob import ContainerClient

    import pyspark
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    from pyspark.sql.functions import col, asc, desc
    from pyspark.sql.functions import col, to_timestamp, monotonically_increasing_id, to_date, when
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark import SparkContext, SparkConf
    from pyspark.streaming import StreamingContext

    from faker import Faker
    from faker import Faker

    import findspark

    print("ok.....")
except Exception as e:
    print("Error : {} ".format(e))
#Set file paths and create arguments for spark
out_valid="output/valid"
out_invalid="output/invalid"
spark_version = '3.3.3'
SUBMIT_ARGS = f'--packages ' \
              f'org.apache.hudi:hudi-spark3.3-bundle_2.12:0.12.1,' \
              f'org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},' \
              f'org.apache.kafka:kafka-clients:2.8.1 ' \
              f'pyspark-shell'
os.environ["PYSPARK_SUBMIT_ARGS"] = SUBMIT_ARGS
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
findspark.init()

#provides information for hudi though it wasn't a goal that has been met of yet
db_name = "hudidb"
table_name = "kafke_data"
recordkey = 'emp_id'
precombine = 'ts'
path = f"file:///C:/tmp/spark_warehouse/{db_name}/{table_name}"
method = 'upsert'
table_type = "COPY_ON_WRITE"
BOOT_STRAP = "localhost:9092"
TOPIC = "FirstTopic"
hudi_options = {
    'hoodie.table.name': table_name,
    'hoodie.datasource.write.recordkey.field': 'emp_id',
    'hoodie.datasource.write.table.name': table_name,
    'hoodie.datasource.write.operation': 'upsert',
    'hoodie.datasource.write.precombine.field': 'ts',
    'hoodie.upsert.shuffle.parallelism': 2,
    'hoodie.insert.shuffle.parallelism': 2
}

#Initializes spark and the kafka data frames it will ingest
spark = SparkSession.builder \
    .master("local") \
    .appName("kafka-example") \
    .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
    .config('className', 'org.apache.hudi') \
    .config('spark.sql.hive.convertMetastoreParquet', 'false') \
    .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
    .config('spark.sql.warehouse.dir', path) \
    .getOrCreate()

kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOT_STRAP) \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("includeHeaders", "true") \
    .option("failOnDataLoss", "false") \
    .load()



#Functions for locating the files after creation and moving them into Azure blob storage utilizes the config.yaml file
def load_config_file():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)
    
def get_files(dir):
    with os.scandir(dir) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.startswith("part"):
                yield entry

def load_to_azure(files, connection_string, container_name):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print("Uploading to azure blob")
            
    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        with open(file.path, "rb") as data:
            blob_client.upload_blob(data)
            print(f'{file.name} uploaded')
            print(f'{file.name} deleted from folder')

#Creates a Schema for what we will eventually write out to CSV
def process_batch_message(df, batch_id):
    my_df = df.selectExpr("CAST(value AS STRING) as json")
    schema = StructType(
        [
            StructField("emp_id",StringType(),True),
            StructField("employee_name",StringType(),True),
            StructField("department",StringType(),True),
            StructField("state",StringType(),True),
            StructField("age",IntegerType(),True),
            StructField("salary",IntegerType(),True),
            StructField("bonus",IntegerType(),True)
        ]
    )
    clean_df = my_df.select(from_json(col("json").cast("string"), schema).alias("parsed_value")).select("parsed_value.*")
    if clean_df.count() >0:
        nulled_catching_df = clean_df.replace({'': None})
        no_null_df = nulled_catching_df.filter("`emp_id` is not null and \
                         `employee_name` is not null and \
                         `department` is not null and \
                         `state` is not null and \
                         `age` is not null and \
                         `salary` is not null and \
                         `bonus` is not null and")
        null_df = nulled_catching_df.filter("`emp_id` is not null and \
                         `employee_name` is null or \
                         `department` is null or \
                         `state` is null or \
                         `age` is null or \
                         `salary` is null or \
                         `bonus` is null")
        
        no_null_marked_df = no_null_df.withColumn("nulls", lit(False))
        null_marked_df = null_df.withColumn("nulls", lit(True))

        valid_df = no_null_marked_df.filter(no_null_marked_df.age >= 16)

        underaged_df = no_null_marked_df.filter(no_null_marked_df.age < 16)

        invalid_df = null_marked_df.union(underaged_df)


        #Writes the information into a csv 
        valid_df.write.format("csv"). \
            option("header", "true"). \
            mode("overwrite"). \
            save(out_valid)
        invalid_df.write.format("csv"). \
            option("header", "false"). \
            mode("overwrite"). \
            save(out_invalid)
    print("batch_id : ", batch_id, clean_df.show(truncate=False))


query = kafka_df.writeStream \
    .foreachBatch(process_batch_message) \
    .option("checkpointLocation", "path/to/HDFS/dir") \
    .trigger(processingTime="2 minutes") \
    .start().awaitTermination(timeout=100)

#Gets the files that were created and loads them into azure
config_file = load_config_file()
valid_csv = get_files(config_file["source_folder"]+ "/valid")
invalid_csv = get_files(config_file["source_folder"]+ "/invalid")

load_to_azure(valid_csv, config_file["azure_storage_connectionstring"], config_file["csv_valid_container_name"])
load_to_azure(invalid_csv, config_file["azure_storage_connectionstring"], config_file["csv_invalid_container_name"])
