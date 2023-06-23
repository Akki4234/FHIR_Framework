import pandas as pd
import json,csv
import numpy as np
import boto3
import time
import os
# import path_validation
from Help import parse_arguments, read_file_from_s3, write_file_to_s3

args = parse_arguments()

if args.input:
    input_file_content = read_file_from_s3(args.input)
    print(f"Input file content:\n{input_file_content}")

if args.output:
        # Your main logic here
    output_file_content = "This is the output content"  # Replace with your actual output content
        
        # Write the output file to S3
    write_file_to_s3(args.output, output_file_content)
    print(f"Output file written to: {args.output}")

if args.config:
    config_file_content = read_file_from_s3(args.config)
    print(f"Config file content:\n{config_file_content}")























# s3 = boto3.resource('s3')
# s3 = boto3.client('s3', aws_access_key_id=path_validation.access_key_id, aws_secret_access_key=path_validation.secret_access_key)

# start = time.time()
# def flatten_json(obj):
#     ret={}
#     def flatten(x,flatten_key=""):
#         if type(x) is list:
#             i=0
#             for elem in x:
#                 flatten(elem,flatten_key + str(i)+'_')
#                 i+=1
#         elif type(x) is dict:
#              for current_key in x:
#                  flatten(x[current_key],flatten_key + current_key +'_')
#         else:
#             ret[flatten_key]=x          
#     flatten(obj)        
#     return ret


# #Input_Data
# input_path = path_validation.input_path[5:]
# input_parts = input_path.split("/", 1)
# input_bucket_name = input_parts[0]
# input_key_name = "" if len(input_parts) == 1 else input_parts[1]
# input_response = s3.get_object(Bucket=input_bucket_name, Key=input_key_name)
# input_content = input_response['Body'].read().decode('utf-8')
# input_data = json.loads(input_content)


# nested_obj=input_data 

# df=pd.DataFrame()
# ans=flatten_json(nested_obj)
# for key,value in ans.items():
#     splitted_key=key.split('_')
#     index=0
#     column_name=''
#     for t in splitted_key:
#         if(t.isdigit()): 
#             index=index+int(t)
#         else:
#            column_name=column_name+t+'_'
#     # print(column_name)
#     # print(index)
#     if column_name not in df.columns:
#         df[column_name]=np.nan
#     df.loc[index,column_name]=value

# mid = time.time()
# print(f'flatten_time: {mid - start}')


# #Config_Data
# config_path = path_validation.config_path[5:]
# config_parts = config_path.split("/", 1)
# config_bucket_name = config_parts[0]
# config_key_name = "" if len(config_parts) == 1 else config_parts[1]
# config_response = s3.get_object(Bucket=config_bucket_name, Key=config_key_name)
# config_content = config_response['Body'].read().decode('utf-8')
# config_file = json.loads(config_content)


# aries={}
# mapping=config_file['column_mapping']
# for i in range(1,len(mapping)+1):
#     try:
#         key=mapping[f'column_{i}']
#         key_name=key["column_name"]
#         mapping_rule=key['Mapping_rule']
#         column_type=key["columntype"]
#     except KeyError:
#         print("Error: Please Check Config file and run again....")
#         exit()    
#     result_array = np.array([])

#     if 'target_column' in mapping_rule and 'match_value' not in mapping_rule and 'source_column' not in mapping_rule:
#         target_column = mapping_rule['target_column'] +'__'
#         if(target_column in df.columns):  
#             for index, row in df.iterrows():
#                 if not pd.isna(row[target_column]):
#                     result_array = np.append(result_array, row[target_column])
#         aries[key_name]=result_array
#         continue
#     try:
#         source_column=mapping_rule['source_column'] +'__'
#         match_value=mapping_rule['match_value']
#         target_column=mapping_rule['target_column'] +'__'
#     except KeyError:
#         print("Error: Please Check Config file and run again....")
#         exit()  
#     result_array = np.array([])
#     if(source_column in df.columns and target_column in df.columns):    
#         for index, row in df.iterrows():
#             if row[source_column] == match_value:
#                 result_array = np.append(result_array, row[target_column])
#     aries[key_name]=result_array 

# result_df=pd.DataFrame()
# result_df = pd.DataFrame(columns=aries.keys(), index=[0])
# for key, value in aries.items():
#     result_df.loc[0, key] = value.tolist()
# for i in range(1,len(mapping)+1):
#     key2=mapping[f'column_{i}']
#     if 'columntype' in key2:
#         if(key2['columntype']=="Nonarray"):
#             result_df=result_df.explode(key2["column_name"])




# #Target_Data
# target_path = path_validation.target_path[5:]
# target_parts = target_path.split("/", 1)
# target_bucket_name = target_parts[0]
# target_key_name = "" if len(target_parts) == 1 else target_parts[1]
# csv_file = open('temp.csv', 'w', newline='')
# csv_writer = csv.writer(csv_file)
# csv_writer.writerows(result_df)
# csv_file.close()
# s3.upload_file('temp.csv', target_bucket_name, target_key_name)


# end = time.time()
# print(f'Explode and Mapping time: {end - start}')