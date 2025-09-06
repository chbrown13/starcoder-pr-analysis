import requests
import csv
from datetime import datetime
import os
from dotenv import load_dotenv
import time

load_dotenv()

repo_v1 = {}
repo_v2 = {}

with open('starcoder_v1_repos.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        repo_v1[row['repo_name']] = row['commit_hash']

with open('starcoder_v2_repos.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        repo_v2[row['repo_name']] = row['commit_hash']

overlapped_repos = []

for repo_name, v1_hash in repo_v1.items():
    if repo_name in repo_v2:
        v2_hash = repo_v2[repo_name]

        repo_data = {
            'repo_name': repo_name,
            'v1_hash': v1_hash,
            'v2_hash': v2_hash
        }
        overlapped_repos.append(repo_data)

repo_dates = []

headers = {
    'Accept': 'application/vnd.github.v3+json',
    'Authorization': 'Bearer ' + os.getenv('GITHUB_PAT'),
    'User-Agent': 'STARCODER ANALYSIS APP',
    'X-GitHub-Api-Version': '2022-11-28',
}

for repo_data in overlapped_repos:
    repo_name = repo_data['repo_name']
    v1_hash = repo_data['v1_hash']
    v2_hash = repo_data['v2_hash']

    repo_meta_data = {"repo_name": repo_name,
                      "v1_hash": v1_hash,
                      "v2_hash": v2_hash
                      }

    try:
        v1_repo_url = f"https://api.github.com/repos/{repo_name}/commits/{v1_hash}"
        response_v1 = requests.get(v1_repo_url, headers=headers)
        time.sleep(1) #pausing to avoid hitting rate limits from GitHub REST API

        response_v1.raise_for_status()

        commit_data_v1 = response_v1.json()
        date_string_v1 = commit_data_v1['commit']['committer']['date']
        # formatting date for better usage!
        v1_date = datetime.fromisoformat(date_string_v1.replace('Z', '+00:00'))

        repo_meta_data["v1_date"] = v1_date

    #v2 data
        v2_repo_url = f"https://api.github.com/repos/{repo_name}/commits/{v2_hash}"
        response_v2 = requests.get(v2_repo_url, headers=headers)
        time.sleep(1)  # pausing to avoid hitting rate limits from GitHub REST API

        response_v2.raise_for_status()

        commit_data_v2 = response_v2.json()
        date_string_v2 = commit_data_v2['commit']['committer']['date']
        # formatting date for better usage!
        v2_date = datetime.fromisoformat(date_string_v2.replace('Z', '+00:00'))

        repo_meta_data["v2_date"] = v2_date

        repo_dates.append(repo_meta_data)
        print(f"Processed dates for {repo_name}.")
    except requests.exceptions.HTTPError as err:
        print(f"{repo_name} did not respond successfully... Skipping due to error: {err}")
    except KeyError as err:
        print(f"Skipping due to missing key: {err}")


all_merged_prs = []

for repo_meta_data in repo_dates:
    repo_name = repo_meta_data['repo_name']
    v1_date = repo_meta_data['v1_date']
    v2_date = repo_meta_data['v2_date']

    url = f"https://api.github.com/repos/{repo_name}/pulls"

    params = {
        'state': 'closed',
        'sort': 'updated',
        'direction': 'desc',
        'per_page': 100
    }
    next_page_available = True
    while next_page_available:
        try:
            response = requests.get(url, headers=headers, params=params)
            time.sleep(1) #throttle

            response.raise_for_status()

            pull_requests = response.json()

            for pull_request in pull_requests:
                if pull_request['merged_at']: #only include pull requests which were merged
                    merge_date = datetime.fromisoformat(pull_request['merged_at'].replace('Z', '+00:00')) # formatting date

                    if v1_date < merge_date < v2_date: #check if the merged pull request is relevant to our data
                        all_merged_prs.append({
                            'repo_name': repo_name,
                            'pr_number': pull_request['number'],
                            'pr_title': pull_request['title'],
                            'pr_url': pull_request['html_url'],
                            'merge_date': merge_date
                        })
            if 'next' in response.links:
                url = response.links['next']['url']
                params = {} #reset params to avoid errors (params already preset from url)
            else:
                next_page_available = False
        except requests.exceptions.HTTPError as err:
            print(f"{repo_name} did not respond successfully... Skipping due to error: {err}")
            next_page_available = False

print(f"Found {len(all_merged_prs)} merged PRs.")
print(f"Saving results to merged_prs.csv")

with open('merged_prs.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['repo_name', 'pr_number', 'pr_title', 'pr_url', 'merge_date'])
    for pr in all_merged_prs:
        writer.writerow([pr[key] for key in ['repo_name', 'pr_number', 'pr_title', 'pr_url', 'merge_date']])


print(f"Finished saving results to merged_prs.csv")
