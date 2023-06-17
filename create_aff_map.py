import boto3
import json

# Creates mapping of institution RID to affiliation name as in ROR database from S2AFF process

# RUN THIS ONE BEFORE process_signatures.py BUT AFTER create_ror_map.py!

with open('ror_map.json', 'r') as f:  # map ror links to institution names
    ror_map = json.load(f)

# reads a line at a time from an s3 bucket JSON file
def read_file_from_s3(bucket_name, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.splitlines():
        if line:
            json_line = json.loads(line)
            yield json_line

bucket_name = 'scigami-bucket'  # Replace with your bucket name
folder_name = 'affiliation_mappings'  # Replace with your folder name

s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')

# Get the list of all objects in the bucket
pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)

# loop over all JSON files in the S3 bucket
aff_map = {}
for page in pages:
    for obj in page['Contents']:
        file_name = obj['Key']
        if file_name.endswith('.json'):
            print("READING FILE:", file_name)
            for line_num, file_content in enumerate(read_file_from_s3(bucket_name, file_name)):
                aff_map[file_content['rid']] = ror_map[file_content['source_id']]  # This mapping should never fail!
                if line_num % 1000000 == 0:
                    print("LINE NUMBER:", line_num)


print("FINAL SIZE OF RESULT:", len(aff_map))

with open(f'aff_map.json', 'w') as f:
    json.dump(aff_map, f)
