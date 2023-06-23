import json
from pyspark.sql.types import StructType
from pyspark.sql.types import *


schema = schema = StructType([StructField('address', ArrayType(StructType([StructField('city', StringType(), True), \
                  StructField('country', StringType(), True), StructField('district', StringType(), True), \
                  StructField('line', ArrayType(StringType(), True), True), StructField('postalCode', StringType(), True),\
                  StructField('state', StringType(), True), StructField('type', StringType(), True), \
                  StructField('use', StringType(), True)]), True), True), \
                  StructField('extension', ArrayType(StructType([StructField('extension', ArrayType(StructType([StructField('url', StringType(), True),\
                  StructField('valueCodeableConcept', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True)]), True), True)]), True), \
                  StructField('valuePeriod', StructType([StructField('end', StringType(), True), StructField('start', StringType(), True)]), True), \
                  StructField('valueReference', StructType([StructField('reference', StringType(), True)]), True)]), True), True), \
                  StructField('url', StringType(), True)]), True), True), StructField('id', StringType(), True),\
                  StructField('meta', StructType([StructField('lastUpdated', StringType(), True), \
                  StructField('profile', ArrayType(StringType(), True), True), StructField('source', StringType(), True),\
                  StructField('versionId', StringType(), True)]), True), StructField('name', StringType(), True),\
                  StructField('resourceType', StringType(), True), StructField('telecom', ArrayType(StructType([StructField('system', StringType(), True), \
                  StructField('use', StringType(), True), StructField('value', StringType(), True)]), True), True), StructField('text', StructType([StructField('div', StringType(),\
                  True), StructField('status', StringType(), True)]), True),StructField('identifier', ArrayType(StructType([StructField('period', StructType([StructField('end', StringType(), True), \
                  StructField('start', StringType(), True)]), True), StructField('system',StringType(),True), StructField('type', StructType([StructField('coding', ArrayType(StructType([StructField('code', StringType(), True),\
                  StructField('display', StringType(), True), StructField('system', StringType(), True)]), True), True)]), True), \
                  StructField('value', StringType(), True)]), True), True)])
json_schema = json.dumps(schema.jsonValue(), indent=2)



print(json_schema)

# targetschemadict={"1":["resourceType", "id", "meta.versionId", "meta.lastUpdated", "meta.profile","meta.Features","meta.healthdata","extension"],'2':["new_extension.url","new_extension.valueCodeableConcept.coding"]}
# explodedict={"1":["profile","Features","healthdata","extension"],"2":["coding"]}  

# json_object = json.dumps(schema, indent = 4) 
# print(json_object)






