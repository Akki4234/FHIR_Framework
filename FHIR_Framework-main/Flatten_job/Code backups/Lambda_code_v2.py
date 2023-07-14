import numpy as np
import itertools
import pandas as pd
import json
import argparse
import os
import boto3
import io
import botocore.exceptions
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO 
from io import BytesIO
import awswrangler as wr
from datetime import datetime


LIST_TYPES = (list, tuple)


# Error codes
ERROR_JSON_INVALID = 1001
ERROR_JSON_TOO_LARGE = 1002
ERROR_CONFIG_INVALID = 2001
ERROR_COLUMN_NAME_MISSING = 2002
ERROR_MAPPINGS_ARRAY_MISSING=2003
ERROR_MAPPING_RULE_DOCUMENT_MISSING=2004
ERROR_COLUMN_TYPE_MISSING=2005
ERROR_DUPLICATE_COLUMN_NAMES = 2006
ERROR_SOURCE_reference_columnS_MISSING = 2007
ERROR_reference_column_MISSING = 2008

LIST_TYPES = (list, tuple)

# Exception classes
class ExecutionError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message
        super().__init__(f"Error code: {code}, Message: {message}")

class InvalidJSONException(ExecutionError):
    def __init__(self, message):
        super().__init__(ERROR_JSON_INVALID, message)

class JSONTooLargeException(ExecutionError):
    def __init__(self, message):
        super().__init__(ERROR_JSON_TOO_LARGE, message)

class InvalidConfigException(ExecutionError):
    def __init__(self, message):
        super().__init__(ERROR_CONFIG_INVALID, message)

class ColumnMappingInvalidException(ExecutionError):
    def __init__(self,code,message):
        super().__init__(code, message)

class DuplicateColumnNamesException(ExecutionError):
    def __init__(self, duplicate_names):
        message = f"Duplicate column names found: {', '.join(duplicate_names)}"
        super().__init__(ERROR_DUPLICATE_COLUMN_NAMES, message)

class SourceTargetColumnsMissingException(ExecutionError):
    def __init__(self, column_name):
        message = f"Source or target column is missing for column name: {column_name}"
        super().__init__(ERROR_SOURCE_reference_columnS_MISSING, message)

class TargetColumnMissingException(ExecutionError):
    def __init__(self, column_name):
        message = f"Target column is missing for column name: {column_name}"
        super().__init__(ERROR_reference_column_MISSING, message)




def flatten(item, parents=(), join="_"):
    for key, val in item.items():
        path = parents + (key,)
        key = str.join(join, path)

        if isinstance(val, dict) and any(val):
            yield from flatten(val, path, join)
        elif isinstance(val, dict):
            yield (key, None)
        else:
            yield (key, val)

def explode(item):
    lists = (
        [(k, x) for x in v] if any(v) else [(k, None)]
        for k, v in item.items()
        if isinstance(v, LIST_TYPES)
    )

    combos = map(dict, itertools.product(*lists))

    for combo in combos:
        xitem = item.copy()
        xitem.update(combo)
        yield xitem

def Flatten_Explode(item, join="_"):
    
    for expl in explode(item):
        flat = dict(flatten(expl, (), join))

        items = filter(lambda x: isinstance(x, LIST_TYPES), flat.values())
        for item in items:
            yield from Flatten_Explode(flat, join)
            break
        else:
            yield flat

def convert_json_to_small_json(s3, input_bucket, input_file):
    print("Creating Small Json files of input json....")
    response = s3.get_object(Bucket=input_bucket, Key=input_file)
    file_content = response['Body'].read().decode('utf-8')
    data = json.loads(file_content)
    id=data.get('id')
    small_json_data = {}
    for key, value in data.items():
        small_json_data[key] = {key: value}

    return small_json_data, id


def handle_exception(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ExecutionError as e:
            print(f"Execution failed...")
            print(f"Error Code: {e.code}, Message: {e.message}")
            exit()
    return wrapper

@handle_exception
def validate_input_json(s3, input_bucket, input_file):
    print("Input json file validating...")
    response = s3.get_object(Bucket=input_bucket, Key=input_file)
    file_content = response['Body'].read().decode('utf-8')

    try:
        input_data = json.loads(file_content)
    except json.JSONDecodeError as e:
        raise InvalidJSONException(f"Input JSON is not valid: {str(e)}")
    except MemoryError:
        raise JSONTooLargeException("Input JSON is too large to load.")
    print("Input json file validated sucessfully...")
@handle_exception
def validate_config_json(s3, config_bucket, config_file):
    print("Input config file validating...")
    response = s3.get_object(Bucket=config_bucket, Key=config_file)
    file_content = response['Body'].read().decode('utf-8')

    try:
        config_data = json.loads(file_content) 
    except json.JSONDecodeError as e:
        raise InvalidConfigException(f"Config JSON is not valid: {str(e)}")
    column_names = []
    duplicate_column_names = set()
    try:
        mappings=config_data['column_mapping']
    except:
        raise ColumnMappingInvalidException(ERROR_MAPPINGS_ARRAY_MISSING,"Configuration of JSON is \
                                            not valid,Column_mapping array not found in config.. ") 
    try:
        for mapping in mappings:
            column_name = mapping['column_name'].lower()
            if column_name in column_names:
                duplicate_column_names.add(column_name)
            column_names.append(column_name)
    except:
        raise ColumnMappingInvalidException(ERROR_COLUMN_NAME_MISSING,f"Configuration of JSON is \
                                            not valid, Key column name not found in {mapping}...")
    
    if duplicate_column_names:
        raise DuplicateColumnNamesException(duplicate_column_names)
    print("Input config file validated sucessfully...")
@handle_exception
def process_data(s3, input_bucket, input_file, config_bucket, config_file, output_bucket, row_json_data_bucket):
    print("Processing input file....")
    response = s3.get_object(Bucket=config_bucket, Key=config_file)
    file_content = response['Body'].read().decode('utf-8')
    config_data = json.loads(file_content)

    small_json, resource_id = convert_json_to_small_json(s3, input_bucket, input_file)

    df = pd.DataFrame()

    print("Data Flattening in progess...")
    for keys, value in small_json.items():
        df2 = pd.DataFrame(list(Flatten_Explode(value)))
        df = pd.concat([df, df2], axis=1)
    

    aries = {}
    print("Apping mapping on flatteded data")
    mapping = config_data['column_mapping']
    cartesian=[]
    for value in mapping:
        key_name = value["column_name"]
        try:
            
            mapping_rule = value['mapping_rule']
        except:
            raise ColumnMappingInvalidException(ERROR_MAPPING_RULE_DOCUMENT_MISSING,f"Configuration of JSON is not valid, MAPPING RULE document not defined where column name is  {key_name} in Config ") 
        try:
            column_type = value["column_type"].lower()
            if(column_type=='nonarray'):
                cartesian.append(key_name)
        except:
            raise ColumnMappingInvalidException(ERROR_COLUMN_TYPE_MISSING,f"Configuration of JSON is not valid,COLUMN TYPE is not defined where column name is {key_name} in Config.. ")  


        result_array = np.array([])
        if 'reference_column' in mapping_rule and 'match_value' not in mapping_rule and 'source_column' not in mapping_rule:
            reference_column = mapping_rule['reference_column']
            if reference_column in df.columns:
                for index, row in df.iterrows():
                    if not pd.isna(row[reference_column]):
                        result_array = np.append(result_array, row[reference_column])
            aries[key_name] = result_array
            continue
        try:
            source_column = mapping_rule['source_column']
            match_value = mapping_rule['match_value']
        except:
            print(f"Execution failed......")
            print("Error: Please ensure that both the 'source_column' and 'match_value' are either present or absent together for", value["column_name"])
            exit()
        try:
            reference_column = mapping_rule['reference_column']
        except:
            raise TargetColumnMissingException(key_name)
            
        result_array = np.array([])

        if source_column in df.columns and reference_column in df.columns:
            for index, row in df.iterrows():
                if row[source_column] == match_value:
                    result_array = np.append(result_array, row[reference_column])
        result_array=np.unique(result_array)
        aries[key_name] = result_array

    result_df = pd.DataFrame()
    result_df = pd.DataFrame(columns=aries.keys(), index=[0])
    for key, value in aries.items():
        result_df.loc[0, key] = value.tolist()
   

    result_df = result_df.apply(lambda x: x.apply(lambda y: np.nan if isinstance(y, list) and len(y) == 0 else y))
    print("Ready for cartesian product...")
    for colname in cartesian:
        result_df = result_df.explode(colname)



    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    s3_output_path = f"{resource_id}_{timestamp}.csv"

    csv_data = result_df.to_csv(index=False)
    print("Data is ready to load into target location...")
    s3.put_object(Body=csv_data, Bucket=output_bucket, Key=s3_output_path )
    print("Execution successful! Data flattening and mapping completed...")
    print("")
    print(f"Flattened data loaded at: s3://{output_bucket}/{s3_output_path}")
    
    print("Moving data to row bucket...")
    row_path = f"{resource_id}_{timestamp}.json"
    s3.copy_object(
        Bucket=row_json_data_bucket,
        Key=row_path,
        CopySource={
            'Bucket': input_bucket,
            'Key': input_file
        }
    )
    s3.delete_object(Bucket=input_bucket,Key=input_file)
    print(f"Row data loaded at: s3://{output_bucket}/{row_path}")


def lambda_handler(event, context):

    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_file = event['Records'][0]['s3']['object']['key']

    s3 = boto3.client('s3')
    config_bucket='fhir-framework-config' 
    config_file='orgaff_config.json'
    output_bucket='fhirframework-output-bucket'  
    row_json_data_bucket='fhir-framework-row-data' 

    validate_input_json(s3, input_bucket, input_file)
    validate_config_json(s3, config_bucket, config_file)
    process_data(s3, input_bucket, input_file, config_bucket, config_file, output_bucket, row_json_data_bucket)