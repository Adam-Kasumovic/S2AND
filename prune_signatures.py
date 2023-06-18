import json

# Sometimes author ids will be missing with papers from the DB, this gets rid of signatures with those ids

# RUN THIS AFTER create_papers.py!

output_folder = "adam-orcids"

with open(f'data/{output_folder}/{output_folder}_papers.json', 'r') as f:
    all_papers = json.load(f)

with open(f'data/{output_folder}/{output_folder}_signatures.json', 'r') as f:
    signatures = json.load(f)

print("INITIAL SIZE OF SIGNATURES:", len(signatures))

to_remove = set()
for s_id, signature in signatures.items():
    if str(signature['paper_id']) not in all_papers:
        to_remove.add(s_id)

for r in to_remove:
    del signatures[r]

print("FINAL SIZE OF SIGNATURES:", len(signatures))

# Now prune the clusters of those removed signatures
with open(f'data/{output_folder}/{output_folder}_clusters.json', 'r') as f:
    clusters = json.load(f)

print("INITIAL SIZE OF CLUSTERS:", len(clusters))

cluster_to_remove = set()
for c_id, cluster in clusters.items():
    cluster_immediate_to_remove = set()
    for sig_id in cluster['signature_ids']:
        if sig_id in to_remove:  # mark ids for removal
            print(f"Removing signature ID {sig_id} from cluster {c_id}...")
            cluster_immediate_to_remove.add(sig_id)
    for citr in cluster_immediate_to_remove:  # remove ids from the list
        while citr in cluster['signature_ids']:  # remove all occurrences of that id in case
            cluster['signature_ids'].remove(citr)
    if len(cluster['signature_ids']) == 0:  # if list is now empty, mark that cluster for removal
        cluster_to_remove.add(c_id)

for r in cluster_to_remove:
    del clusters[r]

# finally, go through all cluster signatures and make sure that every signature has a cluster signature, creating new clusters as needed
all_cluster_signatures = set()
for c_id, cluster in clusters.items():
    for sig_id in cluster['signature_ids']:
        all_cluster_signatures.add(sig_id)

all_signatures = set(signatures.keys())
missing_signatures = all_signatures - all_cluster_signatures
print("MISSING SIGNATURES COUNT:", len(missing_signatures))

cluster_name_index = 0
for m in missing_signatures:
    cluster_id = f'INVALID_ORCID_{cluster_name_index}'
    clusters[cluster_id] = {"cluster_id": cluster_id, "signature_ids": [m]}
    cluster_name_index += 1

print("FINAL SIZE OF CLUSTERS:", len(clusters))

with open(f'data/{output_folder}/{output_folder}_signatures.json', 'w') as f:
    json.dump(signatures, f)

with open(f'data/{output_folder}/{output_folder}_clusters.json', 'w') as f:
    json.dump(clusters, f)
