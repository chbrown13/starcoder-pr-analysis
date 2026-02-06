import requests
import csv
from datetime import datetime
import os
from dotenv import load_dotenv
import time

load_dotenv()

# === FILTERING CONFIGURATION - Change these to filter PRs ===
TARGET_LANGUAGES = ["JavaScript"] # ["Python", "JavaScript", "TypeScript"]  # Only repos with these languages
TITLE_KEYWORDS = ["fix", "bug", "feature", "update", "refactor"]  # Only PRs with these words in title
BODY_KEYWORDS = ["performance", "optimization", "security", "test"]  # Only PRs with these words in body
LANGUAGE_THRESHOLD = 10  # Minimum percentage of code in target language (10%)
STARS_THRESHOLD = 25  # Minimum stars a repo must have
COLLABORATOR_THRESHOLD = 5  # Minimum collaborators a repo must have

def get_repo_languages(repo_name):
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': 'Bearer ' + os.getenv('GITHUB_TOKEN'),
        'User-Agent': 'STARCODER ANALYSIS APP',
        'X-GitHub-Api-Version': '2022-11-28',
    }
    
    url = f"https://api.github.com/repos/{repo_name}/languages"
    response = requests.get(url, headers=headers)
    time.sleep(1)  # Rate limiting
    
    if response.status_code == 200:
        return response.json()
    return {}

def get_repo_stars(repo_name):
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': 'Bearer ' + os.getenv('GITHUB_TOKEN'),
        'User-Agent': 'STARCODER ANALYSIS APP',
        'X-GitHub-Api-Version': '2022-11-28',
    }
    
    url = f"https://api.github.com/repos/{repo_name}/stargazers"
    response = requests.get(url, headers=headers)
    time.sleep(1)  # Rate limiting
    
    if response.status_code == 200:
        return response.json()
    return {}

def has_target_language(repo_name):
    if not TARGET_LANGUAGES:
        return True  # No filter = include all repos
    
    languages = get_repo_languages(repo_name)
    if not languages:
        return False  # Can't get language info = skip
    
    total_bytes = sum(languages.values())
    
    # Check each target language
    for target_lang in TARGET_LANGUAGES:
        lang_bytes = languages.get(target_lang, 0)
        percentage = (lang_bytes / total_bytes) * 100
        
        if percentage >= LANGUAGE_THRESHOLD:
            return True  # Found enough of this language
    
    return False  # Not enough of any target language  

def has_target_stars(repo_name):
    stars = get_repo_stars(repo_name)
    if not stars:
        return False  # Can't get star info = skip
    
    star_count = len(stars)
    
    return star_count >= STARS_THRESHOLD

def has_targets(repo_name):
    return has_target_language(repo_name) and has_target_stars(repo_name) 

def has_keywords(text, keywords):
    if not keywords:
        return True  # No filter = include all
    
    text_lower = text.lower()
    for keyword in keywords:
        if keyword.lower() in text_lower:
            return True
    return False

repo_v1 = {}
repo_v2 = {}

with open('test_v1_repos.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print("row1:", row)
        repo_v1[row['repo_name'].strip()] = row['commit_hash'].strip()

print(f"Loaded {len(repo_v1)} repos from v1 dataset.")
print(repo_v1)

with open('test_v2_repos.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        print("row2:", row)
        repo_v2[row['repo_name'].strip()] = row['commit_hash'].strip()

print(f"Loaded {len(repo_v2)} repos from v2 dataset.")
print(repo_v2)

overlapped_repos = []
print("Finding overlapped repos...")
for repo_name, v1_hash in repo_v1.items():
    print(f"Checking repo: {repo_name}", repo_name in repo_v2)
    if repo_name in repo_v2:
        v2_hash = repo_v2[repo_name]

        repo_data = {
            'repo_name': repo_name,
            'v1_hash': v1_hash,
            'v2_hash': v2_hash
        }
        overlapped_repos.append(repo_data)
        print(f"Found overlapped repo: {repo_name}")

repo_dates = []

headers = {
    'Accept': 'application/vnd.github.v3+json',
    'Authorization': 'Bearer ' + os.getenv('GITHUB_TOKEN'),
    'User-Agent': 'STARCODER ANALYSIS APP',
    'X-GitHub-Api-Version': '2022-11-28',
}
print(f"Processing {len(overlapped_repos)} overlapped repos for commit dates...", overlapped_repos)
for repo_data in overlapped_repos:
    repo_name = repo_data['repo_name']
    v1_hash = repo_data['v1_hash']
    v2_hash = repo_data['v2_hash']
    # print(repo_data)
    # Check repo filters first
    if not has_targets(repo_name):
        print(f"Skipping {repo_name} - doesn't meet filters")
        continue

    repo_meta_data = {"repo_name": repo_name,
                      "v1_hash": v1_hash,
                      "v2_hash": v2_hash
                      }

    try:
        v1_repo_url = f"https://api.github.com/repos/{repo_name}/commits/{v1_hash}"
        response_v1 = requests.get(v1_repo_url, headers=headers)
        time.sleep(1)  # Rate limiting for GitHub API

        response_v1.raise_for_status()

        commit_data_v1 = response_v1.json()
        date_string_v1 = commit_data_v1['commit']['committer']['date']
        # Format date for better usage
        v1_date = datetime.fromisoformat(date_string_v1.replace('Z', '+00:00'))

        repo_meta_data["v1_date"] = v1_date

    #v2 data
        v2_repo_url = f"https://api.github.com/repos/{repo_name}/commits/{v2_hash}"
        response_v2 = requests.get(v2_repo_url, headers=headers)
        time.sleep(1)  # pausing to avoid hitting rate limits from GitHub REST API

        response_v2.raise_for_status()

        commit_data_v2 = response_v2.json()
        date_string_v2 = commit_data_v2['commit']['committer']['date']
        # Format date for better usage
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
            time.sleep(1)  # Rate limiting

            response.raise_for_status()

            pull_requests = response.json()

            for pull_request in pull_requests:
                if pull_request['merged_at']:  # Only include merged pull requests
                    merge_date = datetime.fromisoformat(pull_request['merged_at'].replace('Z', '+00:00'))
                    print(pull_request)
                    if "bot" in pull_request['user']['login'].lower() or pull_request['user']['type'].lower() == "bot":
                        print(f"Skipping bots: {pull_request['user']['login']}; {pull_request['html_url']}")
                        continue 
                    if v1_date < merge_date < v2_date:  # Check if PR is within date range
                        # Check keyword filters
                        title_ok = True # has_keywords(pull_request['title'], TITLE_KEYWORDS)
                        body_ok = True # has_keywords(pull_request.get('body', ''), BODY_KEYWORDS)
                        
                        if title_ok or body_ok:  # Include if matches title or body keywords
                            all_merged_prs.append({
                                'repo_name': repo_name,
                                'pr_number': pull_request['number'],
                                'pr_title': pull_request['title'],
                                'pr_url': pull_request['html_url'],
                                'merge_date': merge_date
                            })
            if 'next' in response.links:
                url = response.links['next']['url']
                params = {}  # Reset params to avoid errors
            else:
                next_page_available = False
        except requests.exceptions.HTTPError as err:
            print(f"{repo_name} did not respond successfully... Skipping due to error: {err}")
            next_page_available = False

print(f"Found {len(all_merged_prs)} filtered merged PRs.")
print(f"Filters applied:")
print(f"  Languages: {TARGET_LANGUAGES}")
print(f"  Language threshold: {LANGUAGE_THRESHOLD}%")
print(f"  Title keywords: {TITLE_KEYWORDS}")
print(f"  Body keywords: {BODY_KEYWORDS}")
print(f"Saving results to filtered_merged_prs2.csv")

with open('filtered_merged_prs2.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['repo_name', 'pr_number', 'pr_title', 'pr_url', 'merge_date'])
    for pr in all_merged_prs:
        writer.writerow([pr[key] for key in ['repo_name', 'pr_number', 'pr_title', 'pr_url', 'merge_date']])


print(f"Finished saving results to filtered_merged_prs2.csv")
