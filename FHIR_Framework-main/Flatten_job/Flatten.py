import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
import json
from pyspark.sql.functions import explode, explode_outer, col, when, split,  \
    concat_ws, collect_set, udf, element_at, to_date, lit, desc, dense_rank

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("PySpark Read JSON") \
    .getOrCreate()

Input_File=input("Enter The name of Input file or Input Folder: ")
Config_File=input("Enter the name of Config_File or Config Folder: ")

with open(f'/workspaces/FHIR_Framework/Config_files/{Config_File}') as config_file:
    con=json.load(config_file)
schema_temp = con["Source_Schema"]
schema = StructType.fromJson(schema_temp)
input_path =f'/workspaces/FHIR_Framework/Input_data_files/{Input_File}'  
df = spark.read.schema(schema).format("json").option("multiLine", "true").load(input_path)

def Addcolumns(df,colums):
    for colm in colums:
        newvalue=colm.replace('.','_')
        df=df.withColumn(f'{newvalue}',col(colm))
    return df

struct_explode_dict = con["intermediate_column_dict"]
array_explode_dict = con["array_column_dict"]

for i in range(1,(len(struct_explode_dict)+1)):
    df=Addcolumns(df,struct_explode_dict[f'{i}'])            
    for column in array_explode_dict[f'{i}']:
        df = df.withColumn(f"new_{column}",explode_outer(column)).\
        drop(col(f"{column}")).withColumnRenamed(f'new_{column}',f'{column}')

final_target_schema=con["final_target_schema"]
df=df.select(*final_target_schema)
df.show()
print(df.count())
Output_Path=Input_File.split('.')[0]
df.write.mode("overwrite").parquet(f'/workspaces/FHIR_Framework/Output/{Output_Path}')