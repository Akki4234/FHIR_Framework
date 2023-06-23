import boto3
import json


def validate_s3_path(path):
    s3 = boto3.resource('s3')
    bucket_name, file_key = parse_s3_path(path)
    try:
        s3 = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        # content = response['Body'].read().decode('utf-8')
        # json_data = json.loads(content)
        # print(json_data)
        return True
    except Exception as e:
        print(f"Error reading JSON file from S3: {e}")
        return False
    
def parse_s3_path(path):
    if path.startswith("s3://"):
        path = path[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    key = "" if len(parts) == 1 else parts[1]
    return bucket, key


# input_path = input("Enter the input path: ")
# config_file_path = input("Enter the config file path: ")
# target_path = input("Enter the target path: ")

input_path = "s3://fhirframework/source/org_aff"
config_path = "s3://fhirframework/config/org_affconfig"
target_path = "s3://fhirframework/target/"

paths = [input_path, config_path, target_path]

for path in paths:
    print(f"Validating Path:",path)
    if not validate_s3_path(path):
        exit(1)

print("-------------ALL PATHS ARE VALID. PROCEED WITH YOUR LOGIC------------------")
