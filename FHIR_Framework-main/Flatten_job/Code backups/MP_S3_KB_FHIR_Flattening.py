import numpy as np
import itertools
import pandas as pd
import json
import argparse
import os
import uuid
import time
import multiprocessing
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import os

#cd FHIR_Framework-main/Flatten_job
#python MP_KB_FHIR_Flattening_copy.py --input s3://fhirinput/org_aff/ --output s3://fhirinput/output --config s3://fhirinput/Config_method/orgaff_config.json


os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAV477VRBHPFUPJDOQ'
os.environ['AWS_SECRET_ACCESS_KEY'] = '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'

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


def validate_path(path):
    if not os.path.exists(path):
        raise ExecutionError(0, f"Path does not exist: {path}")

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

def convert_json_to_small_json(json_file):

    access_key = 'AKIAV477VRBHPFUPJDOQ'
    secret_key = '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'
    region = 'us-east-1'

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    bucket_name = 'fhirinput'
    response = s3.get_object(Bucket=bucket_name, Key=json_file)
    data = json.loads(response['Body'].read().decode('utf-8'))


    small_json_data = {}
    for key, value in data.items():
        small_json_data[key] = {key: value}

    return small_json_data

# Inputs and path validations
def parse_arguments():
    parser = argparse.ArgumentParser(description='Please give input in the following format')
    parser.add_argument('--input', help='Input path to the input file')
    parser.add_argument('--output', help='Output path to the output file')
    parser.add_argument('--config', help='Config path to the config file')
    return parser.parse_args()

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
def validate_json(json_file):
    with open(json_file) as json_file:
        try:
            json.load(json_file)
        except json.JSONDecodeError as e:
            raise InvalidJSONException(f"Input JSON is not valid: {str(e)}")
        except MemoryError:
            raise JSONTooLargeException("Input JSON is too large to load.")

@handle_exception
def validate_config(config_file):
    access_key = 'AKIAV477VRBHPFUPJDOQ'
    secret_key = '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'
    region = 'us-east-1'

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    bucket_name = 'fhirconfig'
    response = s3.get_object(Bucket=bucket_name, Key=config_file)
    try:
        config_data = json.loads(response['Body'].read().decode('utf-8'))
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


@handle_exception
def process_data(json_file, config_file, output_file):

    access_key = 'AKIAV477VRBHPFUPJDOQ'
    secret_key = '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'
    region = 'us-east-1'

    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )
    bucket_name = 'fhirconfig'
    response = s3.get_object(Bucket=bucket_name, Key=config_file)
    try:
        config_data = json.loads(response['Body'].read().decode('utf-8'))
    except json.JSONDecodeError as e:
        raise InvalidConfigException(f"Config JSON is not valid: {str(e)}")
    
    small_json = convert_json_to_small_json(json_file)

    df = pd.DataFrame()

    for keys, value in small_json.items():
        df2 = pd.DataFrame(list(Flatten_Explode(value)))
        df = pd.concat([df, df2], axis=1)
    

    aries = {}
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

    for colname in cartesian:
        result_df = result_df.explode(colname)

    random_file_name = str(uuid.uuid4())


    bucket_name = 'fhir-output'
    file_path = f'{output_file}/{random_file_name}.csv'
    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(result_df)


    s3 = s3fs.S3FileSystem()

    # Write the Table directly to S3 in Parquet format
    with s3.open(f's3://{bucket_name}/{file_path}', 'wb') as f:
        pq.write_table(table, f)
    end_time = time.time()
    execution_time = end_time - start_time
    print("Execution time:MP ", execution_time, "seconds")

    





def process_file(args):
    json_file, config_file, output_folder_path = args
    validate_json(json_file)
    process_data(json_file, config_file, output_folder_path)


if __name__ == '__main__':

    start_time = time.time()
    args = parse_arguments()
    if args.input == '--help':
        parser = argparse.ArgumentParser(description='Please give input in the following format')
        parser.print_help()

    # validate_config("orgaff_config.json")

    s3 = boto3.client(
    's3',
    region_name='us-east-1',
    aws_access_key_id='AKIAV477VRBHPFUPJDOQ',
    aws_secret_access_key='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'
    )
    bucket_name = 'fhirinput'
    folder_name = 'org_aff'
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
    
    json_files = []

    if 'Contents' in response:
        for file in response['Contents']:
            file_name = os.path.basename(file['Key'])
            if file_name.endswith('.json'):
                file_path = os.path.join(folder_name, file_name)
                json_files.append(file_path)
    pros=[]
    for json_file in json_files:
        
        arguments = [json_file, "orgaff_config.json", "flattened_data"]
        p=multiprocessing.Process(target=process_data, args=arguments)
        p.start()
        pros.append(p)