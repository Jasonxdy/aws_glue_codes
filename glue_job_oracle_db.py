from pyspark.context import SparkContext  
from awsglue.context import GlueContext  
from awsglue.dynamicframe import DynamicFrame  
from awsglue.transforms import *  
import pyspark.sql.functions as F 
from pyspark.sql.functions import col
from pyspark.sql.types import StringType,BooleanType,DateType
import boto3

client = boto3.client('glue')
response = client.get_connection(Name='ERP_connect')
DB_URL=response['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
DB_USERNAME=response['Connection']['ConnectionProperties']['USERNAME']
DB_PASSWORD=response['Connection']['ConnectionProperties']['PASSWORD']

sc = SparkContext() 
glueContext = GlueContext(sc) 
spark = glueContext.spark_session

#ERP_connect
query = f"(SELECT * FROM MTL_CATEGORY_SETS_VL) a"  

# MPRM_connect
# query = f"(SELECT * FROM CUST_INFO) a"  


df = spark.read.format("jdbc").option("url", DB_URL).option("user", DB_USERNAME).option("password", DB_PASSWORD).option("dbtable", query).load()
print((df.count(), len(df.columns)))
print('-------------')

# for col in df.dtypes:
#     print(col[0]+" , "+col[1])
    
# df = df.withColumn('movieId', col('movieId').cast(StringType()))


# for col in df.dtypes:
#     print(col[0]+" , "+col[1])

# df.write.format('parquet').mode('overwrite').option("compression","gzip").save('s3://dbc-hands-on-bucket-aileen/test/')  

# df = spark.read.format("parquet").load('s3://dbc-hands-on-bucket-aileen/test/part-00000-08b6acd2-a174-4806-9642-3486bb640efd-c000.gz.parquet')

# for col in df.dtypes:
#     print(col[0]+" , "+col[1])

