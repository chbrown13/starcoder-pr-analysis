import os
from dotenv import load_dotenv
from huggingface_hub import login
from datasets import load_dataset
import csv
import time

start_time = time.monotonic()

load_dotenv()
HF_TOKEN = os.getenv("HF_TOKEN")

login(token=HF_TOKEN)

Starcoder_V1 = load_dataset("bigcode/the-stack-dedup", streaming=True)
Starcoder_V2 = load_dataset("bigcode/the-stack-v2-dedup", "default", streaming=True)

repo_v1 = {}
repo_v2 = {}

v1_start = time.monotonic()
print("Processing V1 data...")
for row in Starcoder_V1['train']:
    repo_v1[row["max_stars_repo_name"]] = row["max_stars_repo_head_hexsha"]
print(f"Done processing V1 data...")


print("Saving V1 data...")
with open('starcoder_v1_repos.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["repo_name", "commit_hash"])
    for repo_name, hashing in repo_v1.items():
        writer.writerow([repo_name, hashing])
v1_end = time.monotonic()
v1_exec = v1_end - v1_start
print(f"Done saving V1 data... \nTotal V1 execution time: {v1_exec:.2f} seconds")


v2_start = time.monotonic()
print("Processing V2 data...")
for row in Starcoder_V2['train']:
    repo_v2[row["repo_name"]] = row["revision_id"]
print("Done processing V2 data...")


print("Saving V2 data...")
with open('starcoder_v2_repos.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["repo_name", "commit_hash"])
    for repo_name, hashing in repo_v2.items():
        writer.writerow([repo_name, hashing])
v2_end = time.monotonic()
v2_exec = v2_end - v2_start
print(f"Done saving V2 data... \nTotal V2 execution time: {v2_exec:.2f} seconds")


end_time = time.monotonic()
duration = end_time - start_time
print(f"Done processing data; Total execution time: {duration:.2f} seconds.")
