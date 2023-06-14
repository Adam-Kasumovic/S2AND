import json
from collections import defaultdict

output_folder = "adam-orcids"

# TODO: Split this up into pieces so no memory overflow
# Read signatures from DB
# Map author ids to list of affiliations
author_to_affiliations = defaultdict(set)
with open('raw_signatures.json', 'r') as f:
    for signature in f:
        signature = json.loads(signature)

        if signature['relationship_type'] == 'affiliated with':
            author_to_affiliations[signature['author_id']].add(signature['name'])

# Convert set values to lists
a2a = defaultdict(list)
for key, value in author_to_affiliations.items():
    a2a[key] = list(value)

# Produce signatures and clusters JSONs
signature_id = 0
result_signatures = {}
orcid_to_signatures = defaultdict(list)
author_id_to_name = {}
with open('raw_signatures.json', 'r') as f:
    for signature in f:
        signature = json.loads(signature)
        if signature['relationship_type'] == 'authored':
            signature_id += 1
            str_sid = str(signature_id)
            author_id_original = signature['author_id']
            author_id_chunks = author_id_original.split('_')
            author_id = int(author_id_chunks[-3] + author_id_chunks[-1])
            paper_id_val = signature['other_id']
            paper_id = int(paper_id_val[paper_id_val.rfind('_')+1:])
            first_name = signature['first_name']
            middle_name = signature['middle_name']
            last_name = signature['last_name']
            email = signature['email']
            block_list = [first_name, middle_name, last_name]
            block_list = [b.lower() for b in block_list if b is not None]
            block = ' '.join(block_list)
            author_id_to_name[author_id_original] = block.title()
            result_signatures[str_sid] = {"author_id": author_id, "paper_id": paper_id,
                                          "signature_id": str_sid, "author_info":
                                              {"given_block": block, "block": block, "position": 0, "first": first_name,
                                               "middle": middle_name, "last": last_name, "suffix": None,
                                               "affiliations": a2a[author_id_original],
                                               "email": email}}

            orcid_id = "".join([c for c in signature['orcid_id'] if c.isdigit() or c == 'X'])  # some ORCIDs are missing hyphens so just make them all numbers
            if len(orcid_id) == 16:
                orcid_to_signatures[orcid_id].append(str_sid)

# Write signatures to JSON
with open(f'data/{output_folder}/{output_folder}_signatures.json', 'w') as f:
    json.dump(result_signatures, f)

orcid_list = []
for key, value in orcid_to_signatures.items():
    orcid_list.append((key, value))

orcid_list.sort()  # make sure order is the same every time

final_orcid_dict = {}
for tup in orcid_list:
    orcid, signature_ids = tup
    final_orcid_dict[orcid] = {"cluster_id": orcid, "signature_ids": signature_ids}

# Write clusters to JSON
with open(f'data/{output_folder}/{output_folder}_clusters.json', 'w') as f:
    json.dump(final_orcid_dict, f)

with open(f'author_id_mappings.json', 'w') as f:
    json.dump(author_id_to_name, f)
