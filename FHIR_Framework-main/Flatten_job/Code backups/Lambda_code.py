import os
import numpy as np
import itertools
import pandas as pd
import json
import boto3
import io
import botocore.exceptions
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO 
from io import BytesIO
import awswrangler as wr





LIST_TYPES = (list, tuple)



def copy_file_between_buckets(source_bucket, source_file_key, destination_bucket, destination_file_key):
    # Create a client for Amazon S3
    s3 = boto3.client('s3')

    # Copy the file from the source bucket to the destination bucket
    s3.copy_object(
        Bucket=destination_bucket,
        Key=destination_file_key,
        CopySource={
            'Bucket': source_bucket,
            'Key': source_file_key
        }
    )

    # Delete the file from the source bucket
    s3.delete_object(
        Bucket=source_bucket,
        Key=source_file_key
    )

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

def convert_json_to_small_json(bucket_name, json_file):
    s3 = boto3.resource('s3')
    try:
        obj = s3.Object(bucket_name, json_file)
        data = json.loads(obj.get()['Body'].read().decode('utf-8'))
        
        
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print(f"Error: The JSON file '{json_file}' does not exist in the S3 bucket.")
        else:
            print(f"Error: Failed to retrieve the JSON file from the S3 bucket: {e}")
        return None

    small_json_data = {}
    for key, value in data.items():
        small_json_data[key] = {key: value}

    return small_json_data

def lambda_handler(event, context):

    input_bucket = event['Records'][0]['s3']['bucket']['name']
    input_file = event['Records'][0]['s3']['object']['key']
 
    
    
    output_bucket='fhirframework-output-bucket'
    
    
    
    
    
    
    
    
    # Read config file from S3
    try:

        s3 = boto3.client('s3')
        bucket_name = 'fhir-framework-config'
        file_key = 'config/orgaff_config.json'
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        config_data = json.loads(file_content)
    
        
        
        
    except Exception as e:
        
        print(e)
        


    # Read input file from S3
    response = s3.get_object(Bucket=input_bucket, Key=input_file)

    
    file_content = response['Body'].read().decode('utf-8')
    input_data = json.loads(file_content)
    
    try:
        response = s3.get_object(Bucket=input_bucket, Key=input_file)
        file_content = response['Body'].read().decode('utf-8')
        input_data = json.loads(file_content)
        
    except botocore.exceptions.ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {
                'statusCode': 400,
                'body': 'Error: The input file does not exist in the S3 bucket.'
            }
        else:
            print(e)
            return {
                'statusCode': 400,
                'body': f"Error: Failed to retrieve the input file from the S3 bucket: {e}"
            }
    
    
    
    
    
    
    # Convert JSON to small JSON
    small_json = convert_json_to_small_json(input_bucket, input_file)
    if small_json is None:
        return {
            'statusCode': 400,
            'body': 'Error: Failed to convert the JSON file to small JSON.'
        }

    df = pd.DataFrame()
    for keys, value in small_json.items():
        df2 = pd.DataFrame(list(Flatten_Explode(value)))
        df = pd.concat([df, df2], axis=1)
    print(df.head(5))
    # Process column mapping
    aries = {}
    mapping = config_data['column_mapping']
    for i in range(1, len(mapping) + 1):
        try:
            key = mapping[f'column_{i}']
            key_name = key["column_name"]
            mapping_rule = key['Mapping_rule']
            column_type = key["columntype"]
        except KeyError:
            return {
                'statusCode': 400,
                'body': 'Error: Please check the config file and run again.'
            }

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
            target_column = mapping_rule['target_column']
        except KeyError:
            return {
                'statusCode': 400,
                'body': 'Error: Please check the config file and run again.'
            }

        result_array = np.array([])
        if source_column in df.columns and target_column in df.columns:
            for index, row in df.iterrows():
                if row[source_column] == match_value:
                    result_array = np.append(result_array, row[target_column])
        aries[key_name] = result_array

    result_df = pd.DataFrame(columns=aries.keys(), index=[0])
    for key, value in aries.items():
        result_df.loc[0, key] = value.tolist()
    for i in range(1, len(mapping) + 1):
        key2 = mapping[f'column_{i}']
        if 'columntype' in key2:
            if key2['columntype'] == "Nonarray":
                result_df = result_df.explode(key2["column_name"])
                

    
    
    
    
    

    # Save output to S3 in Parquet format
    try:
        # csv_buffer = StringIO()
        # result_df.to_csv(csv_buffer)
        # s3_resource = boto3.resource('s3')
        # s3_resource.Object(output_bucket, 'csv_df.csv').put(Body=csv_buffer.getvalue())
        
        out_buffer = BytesIO()
        result_df.to_parquet(out_buffer, index=False)
        s3.put_object(Bucket=output_bucket, Key="temp.parquet", Body=out_buffer.getvalue())
        
        # result_df.to_parquet('df.parquet.gzip',compression='gzip')
        # pd.read_parquet('df.parquet.gzip')
        
        # wr.s3.to_parquet(
        #     dataframe=result_df,
        #     path="s3://fhirfra    mework-output-bucket/key/my-file.parquet"
        # )
        
        print("Output data loaded into s3....")
        
        
        
    
    except botocore.exceptions.ClientError as e:
        print("Error while loading output data into S3 bucket....")
        return {
            'statusCode': 400,
            'body': f"Error: Failed to save the output file to the S3 bucket: {e}"
        }
    
    
    copy_file_between_buckets(
        input_bucket,
        input_file,
        'fhir-framework-row-data',
        'temp.json'
    )
        
    