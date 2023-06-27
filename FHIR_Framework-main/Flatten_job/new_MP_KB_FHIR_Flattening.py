import numpy as np
import itertools
import pandas as pd
import json
import argparse
import os
import time
from multiprocessing import Pool

# Error codes (unchanged)
ERROR_JSON_INVALID = 1001
ERROR_JSON_TOO_LARGE = 1002
ERROR_CONFIG_INVALID = 2001
ERROR_COLUMN_NAME_MISSING = 2002
ERROR_MAPPINGS_ARRAY_MISSING=2003
ERROR_MAPPING_RULE_DOCUMENT_MISSING=2004
ERROR_COLUMN_TYPE_MISSING=2005
ERROR_DUPLICATE_COLUMN_NAMES = 2006
ERROR_SOURCE_TARGET_COLUMNS_MISSING = 2007
ERROR_TARGET_COLUMN_MISSING = 2008

start_time = time.time()
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
        super().__init__(ERROR_SOURCE_TARGET_COLUMNS_MISSING, message)

class TargetColumnMissingException(ExecutionError):
    def __init__(self, column_name):
        message = f"Target column is missing for column name: {column_name}"
        super().__init__(ERROR_TARGET_COLUMN_MISSING, message)

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
    with open(json_file, 'r') as file:
        data = json.load(file)

    small_json_data = {}
    for key, value in data.items():
        small_json_data[key] = {key: value}

    return small_json_data

def validate_json(json_file):
    with open(json_file) as json_file:
        try:
            json.load(json_file)
        except json.JSONDecodeError as e:
            raise InvalidJSONException(f"Input JSON is not valid: {str(e)}")
        except MemoryError:
            raise JSONTooLargeException("Input JSON is too large to load.")

def validate_config(config_file):
    with open(config_file) as config_file1:
        try:
            config_data = json.load(config_file1)
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

def process_data(json_file, config_file, output_file):
    with open(config_file) as config_file1:
        config_data = json.load(config_file1)
    
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
        if 'target_column' in mapping_rule and 'match_value' not in mapping_rule and 'source_column' not in mapping_rule:
            target_column = mapping_rule['target_column']
            if target_column in df.columns:
                for index, row in df.iterrows():
                    if not pd.isna(row[target_column]):
                        result_array = np.append(result_array, row[target_column])
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
            target_column = mapping_rule['target_column']
        except:
            raise TargetColumnMissingException(key_name)
            
        result_array = np.array([])

        if source_column in df.columns and target_column in df.columns:
            for index, row in df.iterrows():
                if row[source_column] == match_value:
                    result_array = np.append(result_array, row[target_column])
        result_array=np.unique(result_array)
        aries[key_name] = result_array

    result_df = pd.DataFrame()
    result_df = pd.DataFrame(columns=aries.keys(), index=[0])
    for key, value in aries.items():
        result_df.loc[0, key] = value.tolist()
   

    result_df = result_df.apply(lambda x: x.apply(lambda y: np.nan if isinstance(y, list) and len(y) == 0 else y))

    for colname in cartesian:
        result_df = result_df.explode(colname)




    # Rest of the processing logic...

    # Save the result to the output file
    result_df.to_csv(output_file)
    print("Execution successful! Data flattening and mapping completed...")
    print("Flattened data loaded at the following path:")
    print(output_file)

def process_files(args):
    json_file, config_file, output_file = args
    validate_json(json_file)
    validate_config(config_file)
    process_data(json_file, config_file, output_file)

def parse_arguments():
    parser = argparse.ArgumentParser(description='Please give input in the following format')
    parser.add_argument('--input', help='Input path to the input folder')
    parser.add_argument('--output', help='Output path to the output folder')
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
def run_multiple_instances(input_folder, output_folder, config_file):
    input_files = [os.path.join(input_folder, file) for file in os.listdir(input_folder) if file.endswith('.json')]
    output_files = [os.path.join(output_folder, file) for file in os.listdir(input_folder) if file.endswith('.json')]
    config_files = [config_file] * len(input_files)

    # Ensure the lengths of input, output, and config files are the same
    if len(input_files) != len(output_files) or len(input_files) != len(config_files):
        raise ExecutionError(0, "The number of input, output, and config files must be the same")

    # Create a list of argument tuples for each instance
    args_list = [(input_file, config_file, output_file) for input_file, output_file, config_file in zip(input_files, output_files, config_files)]

    # Process the files using multiprocessing
    with Pool() as pool:
        pool.map(process_files, args_list)

args = parse_arguments()
if args.input == '--help':
    parser = argparse.ArgumentParser(description='Please give input in the following format')
    parser.print_help()
else:
    input_folder = args.input
    config_file = args.config
    output_folder = args.output

for path in [input_folder, config_file, output_folder]:
    validate_path(path)

run_multiple_instances(input_folder, output_folder, config_file)


#python new_MP_KB_FHIR_Flattening.py --input /workspaces/FHIR_Framework/FHIR_Framework-main/Input_data_files/Device --output /workspaces/FHIR_Framework/FHIR_Framework-main/output --config /workspaces/FHIR_Framework/FHIR_Framework-main/Config_method2/device_config.json
