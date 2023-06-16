import json

# THIS ONE CAN BE RUN MORE THAN ONCE ON A PAPERS JSON--ABSTRACTS WILL BE REPLACED

output_folder = "adam-orcids"

with open(f'data/{output_folder}/{output_folder}_papers.json', 'r') as f:
    all_papers = json.load(f)

with open('abstracts.json', 'r') as f:
    abstracts = json.load(f)

print("INITIAL SIZE OF RESULT:", len(all_papers))

# loop over all JSON files in the S3 bucket and populate the papers dictionary that S2AND needs
added = 0  # total references added
for paper_id, abstract in abstracts.items():
    if paper_id in all_papers:
        all_papers[paper_id]['abstract'] = abstract
        added += 1
        if added % 10000 == 0:
            print("ADDED:", added)

print("FINAL SIZE OF RESULT:", len(all_papers))  # should be unchanged
print("ABSTRACTS ADDED:", added)

with open(f'data/{output_folder}/{output_folder}_papers.json', 'w') as f:
    json.dump(all_papers, f)
