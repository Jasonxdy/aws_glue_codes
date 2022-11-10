from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions 
from awsglue.context import GlueContext  
from awsglue.dynamicframe import DynamicFrame  
from awsglue.transforms import *  
import pyspark.sql.functions as F 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
import boto3
import sys

CONNECT_NAME='ERP_connect'

client = boto3.client('glue')
response = client.get_connection(Name=CONNECT_NAME)
DB_URL=response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
DB_USERNAME=response['Connection']['ConnectionProperties']['USERNAME']
DB_PASSWORD=response['Connection']['ConnectionProperties']['PASSWORD']


sc = SparkContext() 
glueContext = GlueContext(sc) 
spark = glueContext.spark_session


tb_nm=['RCV_TRANSACTIONS']

for TABLE_NAME in tb_nm:
    query = f"(SELECT TRANSACTION_TYPE,TRANSACTION_DATE,SHIPMENT_HEADER_ID,SHIPMENT_LINE_ID,SOURCE_DOCUMENT_CODE,UOM_CODE,PO_HEADER_ID,PO_RELEASE_ID,PO_LINE_ID,PO_LINE_LOCATION_ID,VENDOR_ID,VENDOR_SITE_ID,ORGANIZATION_ID,SUBINVENTORY,LOCATOR_ID FROM {TABLE_NAME}) a"  
    # print (query)
    
    df = spark.read.format("jdbc").option("url", DB_URL).option("user", DB_USERNAME).option("password", DB_PASSWORD).option("dbtable", query).load()
    # print(df.head(10))
    print((df.count(), len(df.columns)))
    print('-------------')
    
    df.write.format('csv').option("compression","gzip").partitionBy('TRANSACTION_DATE').save(f"s3://hl-sf-poc/ods/{TABLE_NAME}/",header='true')
    print ('successed')

