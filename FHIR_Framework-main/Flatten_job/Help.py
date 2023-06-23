import argparse
import boto3

def parse_arguments():
    parser = argparse.ArgumentParser(description='Your program description here')

    # Add your program's arguments
    parser.add_argument('--input', help='S3 path to the input file')
    parser.add_argument('--output', help='S3 path to the output file')
    parser.add_argument('--config', help='S3 path to the config file')

    return parser.parse_args()

def read_file_from_s3(s3_path):
    print(s3_path)


    s3 = boto3.client('s3')
    bucket, key = s3_path.split('/', 3)[2:]
    response = s3.get_object(Bucket=bucket, Key=key)
    file_content = response['Body'].read().decode('utf-8')
    return file_content

def write_file_to_s3(s3_path, file_content):
    s3 = boto3.client('s3')
    bucket, key = s3_path.split('/', 3)[2:]
    s3.put_object(Body=file_content.encode('utf-8'), Bucket=bucket, Key=key)

def main():
    

    # Your program logic here
    # Use args.input, args.output, and args.config to access the provided S3 paths

    # Example:
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
