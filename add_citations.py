import boto3
import json

# DO NOT RUN THIS MORE THAN ONCE (SUCCESSFULLY) ON A PAPERS JSON--DUPLICATES WILL BE MADE

output_folder = "adam-orcids"

with open(f'data/{output_folder}/{output_folder}_papers.json', 'r') as f:
    all_papers = json.load(f)

print("INITIAL SIZE OF RESULT:", len(all_papers))

existing_citations = set()

# reads a line at a time from an s3 bucket JSON file
def read_file_from_s3(bucket_name, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.splitlines():
        if line:
            json_line = json.loads(line)
            x_rid = json_line['x_rid'][-8:]
            source_id = json_line['source_id']
            if x_rid in all_papers and source_id in all_papers and len(source_id) == 8 \
                    and (x_rid, source_id) not in existing_citations:  # only include those with matching papers not seen yet
                existing_citations.add((x_rid, source_id))
                yield x_rid, source_id  # x_rid cited source_id

bucket_name = 'scigami-bucket'  # Replace with your bucket name
folder_name = 'citations'  # Replace with your folder name

s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')

# Get the list of all objects in the bucket
pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)

# loop over all JSON files in the S3 bucket and populate the papers dictionary that S2AND needs
added = 0  # total references added
for page in pages:
    for obj in page['Contents']:
        file_name = obj['Key']
        if file_name.endswith('.json'):
            print("READING FILE:", file_name)
            for line_num, file_content in enumerate(read_file_from_s3(bucket_name, file_name)):
                all_papers[file_content[0]]['references'].append(int(file_content[1]))
                added += 1


print("FINAL SIZE OF RESULT:", len(all_papers))  # should be unchanged
print("REFERENCES ADDED:", added)

with open(f'data/{output_folder}/{output_folder}_papers.json', 'w') as f:
    json.dump(all_papers, f)
