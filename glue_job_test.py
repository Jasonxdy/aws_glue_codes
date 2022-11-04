import requests
import boto3

URL = "https://jsonplaceholder.typicode.com/photos"
r = requests.get(url = URL)

s3_client = boto3.client('s3', region_name='ap-northeast-2')
s3_client.put_object(Body=r.text, Bucket='test-bucket-dyc', Key='data/photos.json')



from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql import *
from pyspark.sql.types import DateType
from pyspark.sql.types import StringType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read\
    .option("multiline", "true")\
    .json("s3://test-bucket-dyc/data/photos.json")
    
S3_location = "s3://test-bucket-dyc/data/test/"
##df.write.mode('overwrite').save(S3_location)

df.write.option("compression", "gzip").csv(S3_location) 
