import numpy as np
import itertools
import pandas as pd
import json
import argparse
import os
LIST_TYPES = (list, tuple)

def validate_path(path):
    if not os.path.exists(path):
        print(f"Execution failed......")
        print("Path does not exist:", path)
        exit()


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

# Inputs and path validations
def parse_arguments():
    parser = argparse.ArgumentParser(description='Please give input in the following format')
    parser.add_argument('--input', help='Input path to the input file')
    parser.add_argument('--output', help='Output path to the output file')
    parser.add_argument('--config', help='Config path to the config file')
    return parser.parse_args()


args = parse_arguments()
if args.input == '--help':
    parser = argparse.ArgumentParser(description='Please give input in the following format')
    parser.print_help()
else:
    json_file_path = args.input
    config_file = args.config
    output_file_path = args.output

paths = [json_file_path, config_file, output_file_path.rsplit('/', 1)[0]]
for path in paths:
    validate_path(path)


with open(json_file_path) as json_file:
    try:
        json_data = json.load(json_file)
    except json.JSONDecodeError as e:
        print(f"Execution failed......")
        print("Input JSON is not valid:", str(e))
        exit()
    except MemoryError:
        print(f"Execution failed......")
        print("Input JSON is too large to load.")
        exit()

with open(config_file) as config_file1:

        try:
            config_data = json.load(config_file1)
        except json.JSONDecodeError as e:
            print(f"Execution failed......")
            print("Iconfig JSON is not valid:", str(e))
            exit()
            
        column_names = []
        duplicate_column_names = set()
        try:
            for mapping in config_data['column_mapping']:
                column_name = mapping['column_name'].lower()
                if column_name in column_names:
                    duplicate_column_names.add(column_name)
                column_names.append(column_name)
        except:
            print(f"Execution failed......")
            print("Configuration of JSON is not valid, Key column name not found...")
            exit()
            # Check for duplicates
        if duplicate_column_names:
            print(f"Execution failed......")
            print("Duplicate column names found:", duplicate_column_names)
            exit()

    

small_json = convert_json_to_small_json(json_file_path)
with open('/workspaces/FHIR_Framework/output/intermidate_results/small_json.json', 'w') as json_file:
    json.dump(small_json, json_file, indent=4)

df = pd.DataFrame()

for keys, value in small_json.items():
    df2 = pd.DataFrame(list(Flatten_Explode(value)))
    df = pd.concat([df, df2], axis=1)
df.to_csv("/workspaces/FHIR_Framework/output/intermidate_results/intermediate_data.csv")



aries = {}
mapping = config_data['column_mapping']
cartesian=[]
for value in mapping:
    try:
        key_name = value["column_name"]
        mapping_rule = value['mapping_rule']
        column_type = value["column_type"].lower()
        if(column_type=='nonarray'):
            cartesian.append(key_name)

    except KeyError:
        print(f"Execution failed......")
        print("Error: Please check the mapping in the config file for", value["column_name"])
        exit()

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
    except KeyError:
        print(f"Execution failed......")
        print("Error: target_column is mandatory key, Please Check the Config file for ", value["column_name"])
        exit()
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

result_df.to_csv("/workspaces/FHIR_Framework/output/intermidate_results/array_columns.csv")

result_df = result_df.apply(lambda x: x.apply(lambda y: np.nan if isinstance(y, list) and len(y) == 0 else y))

for colname in cartesian:
    result_df = result_df.explode(colname)


result_df.to_csv(output_file_path)
print("Execution successful! Data flattening and mapping completed...")
print("Flattened data loaded at the following path:")
print(output_file_path)



#python FHIR_Flattening.py --input /workspaces/FHIR_Framework/Input_data_files/observation/observation_2.json --output /workspaces/FHIR_Framework/output/output.csv --config /workspaces/FHIR_Framework/Config_method2/observation_config.json
