import boto3
import requests
import json
import time
from concurrent.futures import ProcessPoolExecutor

# Creates mapping of ROR link to affiliation name as in ROR database from S2AFF process

# RUN THIS ONE BEFORE create_aff_map.py and (therefore before) process_signatures.py!

# reads a line at a time from an s3 bucket JSON file
def read_file_from_s3(bucket_name, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.splitlines():
        if line:
            json_line = json.loads(line)
            yield json_line['source_id']


def fetch(ror):
    ror_id = ror[1][-9:]
    retries = 0
    # failed = False
    while retries < 10:
        try:
            response = requests.get(f'https://api.ror.org/organizations/{ror_id}')
            name = json.loads(response.text)['name']
            if (ror[0] + 1) % 1000 == 0:
                print("REQUESTS DONE:", ror[0]+1)
            return ror[0], name
        except:
            # if not failed:
            #     print("REQUEST FAILED!")
            #     failed = True
            time.sleep(min(60, 2 ** retries))  # Wait for progressively longer periods
            retries += 1
    print(f"INSTITUTION NAME NOT FOUND AT {ror[0]} WITH ROR ID {ror_id}")
    return ror[0], None


def fetch_all(rors):
    with ProcessPoolExecutor() as executor:
        responses = list(executor.map(fetch, rors))
    return responses


bucket_name = 'scigami-bucket'  # Replace with your bucket name
folder_name = 'all_distinct_rors'  # Replace with your folder name

s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')

# Get the list of all objects in the bucket
pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)

# loop over all JSON files in the S3 bucket
ror_map = {}
ror_links = []
insert_num = 0
for page in pages:
    for obj in page['Contents']:
        file_name = obj['Key']
        if file_name.endswith('.json'):
            print("READING FILE:", file_name)
            start_time = time.time()
            for line_num, file_content in enumerate(read_file_from_s3(bucket_name, file_name)):
                ror_links.append((insert_num, file_content))
                insert_num += 1

print("ROR IDS LIST CREATED. SIZE:", len(ror_links))

chunk_size = 1000
times_to_run = (len(ror_links) // chunk_size) + 1
print(f"RUNNING {times_to_run} ITERATIONS WITH CHUNK SIZE {chunk_size}")
responses = []
for i in range(times_to_run):
    responses.extend(fetch_all(ror_links[(i*chunk_size):((i+1)*chunk_size)]))  # fetch multiple at a time
    print(f"ITERATION {i+1} COMPLETE!")

responses.sort()  # make numbers line up with ror_links

for i in range(len(ror_links)):
    ror_map[ror_links[i][1]] = responses[i][1]

print("FINAL SIZE OF RESULT:", len(ror_map))
print("NUMBER OF INSTITUTION NAMES THAT ARE NOT NONE:", sum(value is not None for value in ror_map.values()))

with open(f'ror_map.json', 'w') as f:
    json.dump(ror_map, f)
