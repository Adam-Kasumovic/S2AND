import boto3
import json

# RUN THIS BEFORE RUNNING create_papers.py!

output_folder = "adam-orcids"

# first, get a set of all the papers and name mappings
all_papers = set()

with open('raw_signatures.json', 'r') as f:
    for signature in f:
        signature = json.loads(signature)
        if signature['relationship_type'] == 'authored':
            all_papers.add(signature['other_id'])

print("NUMBER OF UNIQUE PAPERS:", len(all_papers))

# reads a line at a time from an s3 bucket JSON file
def read_file_from_s3(bucket_name, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.splitlines():
        if line:
            json_line = json.loads(line)
            if json_line['rid'] in all_papers and json_line['element_type'] == 'paragraph' and 'Abstract' in json_line['hierarchy_position_text']:  # only include those with matching signatures/papers
                yield json_line  # will be a JSON object

bucket_name = 'scigami-bucket'  # Replace with your bucket name
folder_name = 'papers'  # Replace with your folder name

s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')

# Get the list of all objects in the bucket
pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)

abstracts_dict = {}

# loop over all JSON files in the S3 bucket and populate the papers dictionary that S2AND needs
for page in pages:
    for obj in page['Contents']:
        file_name = obj['Key']
        if file_name.endswith('.json'):
            print("READING FILE:", file_name)
            for line_num, file_content in enumerate(read_file_from_s3(bucket_name, file_name)):
                rid_val = file_content['rid']
                rid = str(int(rid_val[rid_val.rfind('_') + 1:]))  # need to match future paper ids and chop off leading 0
                if rid not in abstracts_dict:
                    abstracts_dict[rid] = file_content['element_content']  # just one paragraph will likely suffice
                if line_num % 10000 == 0:
                    print("LINE NUMBER:", line_num)

            print("SIZE OF RESULT:", len(abstracts_dict))

with open(f'abstracts.json', 'w') as f:
    json.dump(abstracts_dict, f)
