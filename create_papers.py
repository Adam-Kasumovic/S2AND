import boto3
import json

output_folder = "adam-orcids"

# first, get a set of all the papers and name mappings
all_papers = set()

with open('raw_signatures.json', 'r') as f:
    for signature in f:
        signature = json.loads(signature)
        if signature['relationship_type'] == 'authored':
            all_papers.add(signature['other_id'])

with open(f'author_id_mappings.json', 'r') as f:  # get mappings of author ids to names
    author_id_to_name = json.load(f)


# reads a line at a time from an s3 bucket JSON file
def read_file_from_s3(bucket_name, key):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket_name, key)
    file_content = obj.get()['Body'].read().decode('utf-8')
    for line in file_content.splitlines():
        if line:
            json_line = json.loads(line)
            if json_line['rid'] in all_papers and json_line['element_type'] == 'title':  # only include those with matching signatures/papers and titles
                yield json_line  # will be a JSON object

bucket_name = 'scigami-bucket'  # Replace with your bucket name
folder_name = 'papers'  # Replace with your folder name

s3_client = boto3.client('s3')
paginator = s3_client.get_paginator('list_objects_v2')

# Get the list of all objects in the bucket
pages = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)

papers_dict = {}

# loop over all JSON files in the S3 bucket and populate the papers dictionary that S2AND needs
for page in pages:
    for obj in page['Contents']:
        file_name = obj['Key']
        if file_name.endswith('.json'):
            print("READING FILE:", file_name)
            for file_content in read_file_from_s3(bucket_name, file_name):
                rid_val = file_content['rid']
                rid = int(rid_val[rid_val.rfind('_')+1:])  # need to match signatures paper id and chop off leading 0
                year = file_content.get('publication_date')
                if year is not None:
                    year = int(year[:4])
                author_name = author_id_to_name.get(file_content.get('x_rid'))
                authors_list = [{"position": 0, "author_name": author_name}] if author_name is not None else []
                if str(rid) not in papers_dict:  # TODO: Figure out how to get abstract, references from SQL table reliably
                    starting_piece = {"paper_id": rid, "title": file_content.get('element_content'),
                                             "abstract": "", "journal_name": file_content.get('publisher_name'),
                                             "venue": file_content.get('publisher_name'),
                                             "year": year, "references": [],
                                             "authors": authors_list}
                    #print("ADDING STARTING PIECE:", starting_piece)
                    papers_dict[str(rid)] = starting_piece
                elif author_name is not None:
                    papers_dict[str(rid)]['authors'].append({"position": len(papers_dict[str(rid)]['authors']),
                                                             "author_name": author_name})
                    #print("AUTHOR ADDED, PIECE NOW LOOKS LIKE:", papers_dict[str(rid)])

print("SIZE OF RESULT:", len(papers_dict))

with open(f'data/{output_folder}/{output_folder}_papers.json', 'w') as f:
    json.dump(papers_dict, f)
