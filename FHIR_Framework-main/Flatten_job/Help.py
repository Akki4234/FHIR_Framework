# import os
# import shutil

# input_folder_path = '/workspaces/FHIR_Framework/FHIR_Framework-main/Input_data_files/org_aff'  # Replace with the actual input folder path
# file_name = 'org_aff.json'  # Replace with the actual file name (including the extension)

# # Read the file from the input location
# file_path = os.path.join(input_folder_path, file_name)
# with open(file_path, 'rb') as file:
#     file_content = file.read()

# # Create copies of the file with incremented numbers
# output_folder_path = input_folder_path  # Output folder is the same as the input folder
# num_copies = 700 # Number of copies to create

# for i in range(num_copies):
#     new_file_name = f'{"org_aff"}_{i+1}.json'
#     new_file_path = os.path.join(output_folder_path, new_file_name)
#     with open(new_file_path, 'wb') as new_file:
#         new_file.write(file_content)

#     print(f'Copied file: {new_file_path}')

# python Help.py
# _________________________________________________________________________________

import boto3
import os

# AWS credentials and region configuration
aws_access_key_id = 'AKIAV477VRBHPFUPJDOQ'
aws_secret_access_key = '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'
aws_region = 'us-east-1'


# S3 bucket configuration
bucket_name = 'fhirinput'

# Input folder path containing the files
input_folder_path = '/workspaces/FHIR_Framework/FHIR_Framework-main/Input_data_files/org_aff'  # Replace with the actual input folder path
s
# Create an S3 clients
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key,
                  region_name=aws_region)

# Get a list of files in the input folder
file_names = os.listdir(input_folder_path)

# Upload each file to S3
for file_name in file_names:
    file_path = os.path.join(input_folder_path, file_name)
    s3.upload_file(file_path, bucket_name, file_name)

    print(f'Uploaded file: {file_name} to bucket: {bucket_name}')
# _________________________________________________________________________________

# import boto3

# def copy_json_files(source_bucket, destination_bucket, destination_folder, access_key, secret_key, region):
#     s3 = boto3.client(
#         's3',
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         region_name=region
#     )

#     response = s3.list_objects_v2(Bucket=source_bucket)
#     if 'Contents' in response:
#         for obj in response['Contents']:
#             key = obj['Key']
#             if key.endswith('.json'):
#                 destination_key = f"{destination_folder}/{key}"
#                 copy_source = {'Bucket': source_bucket, 'Key': key}
#                 s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)
#                 print(f"Copied {key} to {destination_bucket}/{destination_key}")

# # Specify the source and destination bucket names, destination folder, access key, secret key, and region
# source_bucket = 'fhirconfig'
# destination_bucket = 'fhirinput'
# destination_folder = 'arg_aff'
# access_key = 'AKIAV477VRBHPFUPJDOQ'
# secret_key = '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'
# region = 'us-east-1'

# # Copy the JSON files from the source bucket to the destination bucket under the specified folder
# copy_json_files(source_bucket, destination_bucket, destination_folder, access_key, secret_key, region)

