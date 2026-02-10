import requests
import csv
from datetime import datetime
import os
from dotenv import load_dotenv
import time
import re
import json
from collections import defaultdict
from urllib.parse import urlparse

load_dotenv()

# Configuration
TARGET_LANGUAGES = ["JavaScript", "Python", "TypeScript"]
MIN_STARS = 25
RATE_LIMIT_DELAY = 1  # seconds

class CodeChangeAnalyzer:
    def __init__(self):
        self.headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': 'Bearer ' + os.getenv('GITHUB_TOKEN'),
            'User-Agent': 'STARCODER ANALYSIS APP',
            'X-GitHub-Api-Version': '2022-11-28',
        }
        self.gh_token = os.getenv('GITHUB_TOKEN')
        
    def load_repo_datasets(self):
        """Load V1 and V2 repository datasets"""
        repo_v1 = {}
        repo_v2 = {}
        
        print("Loading V1 and V2 repositories...")
        
        with open('starcoder_v1_repos.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                repo_v1[row['repo_name'].strip()] = row['commit_hash'].strip()
        
        with open('starcoder_v2_repos.csv', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                repo_v2[row['repo_name'].strip()] = row['commit_hash'].strip()
        
        print(f"Loaded {len(repo_v1)} V1 repos and {len(repo_v2)} V2 repos")
        return repo_v1, repo_v2
    
    def find_overlapping_repos(self, repo_v1, repo_v2):
        """Find repositories that appear in both V1 and V2"""
        overlapped = []
        
        print("Finding overlapping repositories...")
        for repo_name, v1_hash in repo_v1.items():
            if repo_name in repo_v2:
                overlapped.append({
                    'repo_name': repo_name,
                    'v1_hash': v1_hash,
                    'v2_hash': repo_v2[repo_name]
                })
        
        print(f"Found {len(overlapped)} overlapping repositories")
        return overlapped
    
    def get_repo_languages(self, repo_name):
        """Get programming languages used in a repository"""
        try:
            url = f"https://api.github.com/repos/{repo_name}/languages"
            response = requests.get(url, headers=self.headers)
            time.sleep(RATE_LIMIT_DELAY)
            
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            print(f"Error getting languages for {repo_name}: {e}")
            return {}
    
    def get_commit_date(self, repo_name, commit_hash):
        """Get the date of a specific commit"""
        try:
            url = f"https://api.github.com/repos/{repo_name}/commits/{commit_hash}"
            response = requests.get(url, headers=self.headers)
            time.sleep(RATE_LIMIT_DELAY)
            
            if response.status_code == 200:
                commit_data = response.json()
                date_string = commit_data['commit']['committer']['date']
                return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return None
        except Exception as e:
            print(f"Error getting commit date for {repo_name}/{commit_hash}: {e}")
            return None
    
    def get_merged_prs(self, repo_name, v1_date, v2_date):
        """Get all merged PRs between v1_date and v2_date"""
        prs = []
        url = f"https://api.github.com/repos/{repo_name}/pulls"
        
        params = {
            'state': 'closed',
            'sort': 'updated',
            'direction': 'desc',
            'per_page': 100
        }
        
        page_count = 0
        max_pages = 10  # Limit to avoid excessive API calls
        
        while url and page_count < max_pages:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                time.sleep(RATE_LIMIT_DELAY)
                response.raise_for_status()
                
                pull_requests = response.json()
                
                for pr in pull_requests:
                    if pr['merged_at']:  # Only merged PRs
                        merge_date = datetime.fromisoformat(pr['merged_at'].replace('Z', '+00:00'))
                        
                        # Skip bots
                        if "bot" in pr['user']['login'].lower() or pr['user']['type'].lower() == "bot":
                            continue
                        
                        # Check if PR is within date range
                        if v1_date < merge_date < v2_date:
                            prs.append({
                                'number': pr['number'],
                                'title': pr['title'],
                                'url': pr['html_url'],
                                'merge_date': merge_date,
                                'author': pr['user']['login'],
                                'additions': pr.get('additions', 0),
                                'deletions': pr.get('deletions', 0),
                                'changed_files': pr.get('changed_files', 0)
                            })
                
                # Check for next page
                if 'next' in response.links:
                    url = response.links['next']['url']
                    params = {}
                    page_count += 1
                else:
                    url = None
                    
            except requests.exceptions.HTTPError as e:
                print(f"Error getting PRs for {repo_name}: {e}")
                break
        
        return prs
    
    def get_pr_files(self, repo_name, pr_number):
        """Get list of files changed in a PR"""
        files = []
        url = f"https://api.github.com/repos/{repo_name}/pulls/{pr_number}/files"
        
        params = {'per_page': 300}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            time.sleep(RATE_LIMIT_DELAY)
            response.raise_for_status()
            
            files = response.json()
            return files
        except Exception as e:
            print(f"Error getting files for PR #{pr_number} in {repo_name}: {e}")
            return []
    
    def analyze_diff(self, diff_text):
        """Analyze a diff to extract metrics"""
        analysis = {
            'code_additions': 0,
            'code_deletions': 0,
            'comment_additions': 0,
            'comment_deletions': 0,
            'lines_added': 0,
            'lines_removed': 0
        }
        
        if not diff_text:
            return analysis
        
        lines = diff_text.split('\n')
        for line in lines:
            if line.startswith('+') and not line.startswith('+++'):
                analysis['lines_added'] += 1
                # Simple heuristic: lines with # or // or /* are comments
                if '#' in line or '//' in line or '/*' in line:
                    analysis['comment_additions'] += 1
                else:
                    analysis['code_additions'] += 1
            elif line.startswith('-') and not line.startswith('---'):
                analysis['lines_removed'] += 1
                if '#' in line or '//' in line or '/*' in line:
                    analysis['comment_deletions'] += 1
                else:
                    analysis['code_deletions'] += 1
        
        return analysis
    
    def get_file_language(self, filename):
        """Determine programming language from file extension"""
        ext_to_lang = {
            '.py': 'Python',
            '.js': 'JavaScript',
            '.ts': 'TypeScript',
            '.jsx': 'JavaScript',
            '.tsx': 'TypeScript',
            '.java': 'Java',
            '.c': 'C',
            '.cpp': 'C++',
            '.cs': 'C#',
            '.php': 'PHP',
            '.rb': 'Ruby',
            '.go': 'Go',
            '.rs': 'Rust',
            '.swift': 'Swift',
            '.kt': 'Kotlin',
            '.scala': 'Scala',
            '.sh': 'Shell',
            '.css': 'CSS',
            '.html': 'HTML',
            '.json': 'JSON',
            '.yaml': 'YAML',
            '.yml': 'YAML',
            '.xml': 'XML',
            '.sql': 'SQL'
        }
        
        _, ext = os.path.splitext(filename.lower())
        return ext_to_lang.get(ext, 'Other')
    
    def categorize_change_type(self, filename, additions, deletions):
        """Categorize the type of change made to a file"""
        if additions == 0:
            return 'deletion'
        elif deletions == 0:
            return 'addition'
        elif additions > 0 and deletions > 0:
            # Bigger change (more than 10 line difference) indicates refactoring
            if abs(additions - deletions) > 10:
                return 'refactoring'
            else:
                return 'modification'
        return 'modification'
    
    def analyze_pr_files(self, repo_name, pr_number, pr):
        """Analyze all files in a PR"""
        pr_analysis = {
            'files_added': 0,
            'files_modified': 0,
            'files_deleted': 0,
            'total_lines_added': 0,
            'total_lines_removed': 0,
            'code_additions': 0,
            'code_deletions': 0,
            'comment_additions': 0,
            'comment_deletions': 0,
            'languages_changed': defaultdict(int),
            'change_types': defaultdict(int),
            'patterns': {
                'imports_added': 0,
                'functions_added': 0,
                'classes_added': 0,
                'test_changes': 0
            }
        }
        
        files = self.get_pr_files(repo_name, pr_number)
        
        for file_info in files:
            filename = file_info['filename']
            language = self.get_file_language(filename)
            additions = file_info.get('additions', 0)
            deletions = file_info.get('deletions', 0)
            status = file_info['status']
            
            # Update file counts
            if status == 'added':
                pr_analysis['files_added'] += 1
            elif status == 'deleted':
                pr_analysis['files_deleted'] += 1
            elif status == 'modified':
                pr_analysis['files_modified'] += 1
            
            # Update line counts
            pr_analysis['total_lines_added'] += additions
            pr_analysis['total_lines_removed'] += deletions
            
            # Track language changes
            pr_analysis['languages_changed'][language] += 1
            
            # Get patch/diff
            patch = file_info.get('patch', '')
            diff_analysis = self.analyze_diff(patch)
            
            pr_analysis['code_additions'] += diff_analysis['code_additions']
            pr_analysis['code_deletions'] += diff_analysis['code_deletions']
            pr_analysis['comment_additions'] += diff_analysis['comment_additions']
            pr_analysis['comment_deletions'] += diff_analysis['comment_deletions']
            
            # Analyze patterns
            change_type = self.categorize_change_type(filename, additions, deletions)
            pr_analysis['change_types'][change_type] += 1
            
            # Pattern detection
            if patch:
                if re.search(r'^\+.*import\s|^\+.*require\(', patch, re.MULTILINE):
                    pr_analysis['patterns']['imports_added'] += 1
                if re.search(r'^\+\s*(def|function|const.*=.*\(|async.*\()', patch, re.MULTILINE):
                    pr_analysis['patterns']['functions_added'] += 1
                if re.search(r'^\+\s*class\s+', patch, re.MULTILINE):
                    pr_analysis['patterns']['classes_added'] += 1
                if 'test' in filename.lower():
                    pr_analysis['patterns']['test_changes'] += 1
        
        return pr_analysis, len(files)
    
    def run_analysis(self):
        """Run the complete code change analysis"""
        print("=" * 80)
        print("STARCODER V1 TO V2 CODE CHANGE ANALYSIS")
        print("=" * 80)
        
        # Load datasets
        repo_v1, repo_v2 = self.load_repo_datasets()
        
        # Find overlapping repos
        overlapped = self.find_overlapping_repos(repo_v1, repo_v2)
        
        all_pr_analysis = []
        processed_repos = 0
        skipped_repos = 0
        
        # Process each overlapping repo
        for repo_data in overlapped:
            repo_name = repo_data['repo_name']
            v1_hash = repo_data['v1_hash']
            v2_hash = repo_data['v2_hash']
            
            print(f"\nProcessing {repo_name}...")
            
            # Get commit dates
            v1_date = self.get_commit_date(repo_name, v1_hash)
            v2_date = self.get_commit_date(repo_name, v2_hash)
            
            if not v1_date or not v2_date:
                print(f"  Skipping: Could not get commit dates")
                skipped_repos += 1
                continue
            
            # Get merged PRs
            prs = self.get_merged_prs(repo_name, v1_date, v2_date)
            print(f"  Found {len(prs)} merged PRs between {v1_date.date()} and {v2_date.date()}")
            
            # Analyze each PR
            for pr in prs:
                pr_number = pr['number']
                print(f"    Analyzing PR #{pr_number}...")
                
                try:
                    pr_files_analysis, file_count = self.analyze_pr_files(
                        repo_name, pr_number, pr
                    )
                    
                    # Combine PR metadata with analysis
                    analysis_record = {
                        'repo_name': repo_name,
                        'v1_commit': v1_hash[:7],
                        'v2_commit': v2_hash[:7],
                        'v1_date': v1_date.isoformat(),
                        'v2_date': v2_date.isoformat(),
                        'pr_number': pr_number,
                        'pr_title': pr['title'],
                        'pr_url': pr['url'],
                        'merge_date': pr['merge_date'].isoformat(),
                        'author': pr['author'],
                        'files_changed': file_count,
                        'api_additions': pr['additions'],
                        'api_deletions': pr['deletions'],
                        'files_added': pr_files_analysis['files_added'],
                        'files_modified': pr_files_analysis['files_modified'],
                        'files_deleted': pr_files_analysis['files_deleted'],
                        'total_lines_added': pr_files_analysis['total_lines_added'],
                        'total_lines_removed': pr_files_analysis['total_lines_removed'],
                        'code_additions': pr_files_analysis['code_additions'],
                        'code_deletions': pr_files_analysis['code_deletions'],
                        'comment_additions': pr_files_analysis['comment_additions'],
                        'comment_deletions': pr_files_analysis['comment_deletions'],
                        'languages_changed': json.dumps(dict(pr_files_analysis['languages_changed'])),
                        'change_types': json.dumps(dict(pr_files_analysis['change_types'])),
                        'imports_added': pr_files_analysis['patterns']['imports_added'],
                        'functions_added': pr_files_analysis['patterns']['functions_added'],
                        'classes_added': pr_files_analysis['patterns']['classes_added'],
                        'test_changes': pr_files_analysis['patterns']['test_changes']
                    }
                    
                    all_pr_analysis.append(analysis_record)
                    
                except Exception as e:
                    print(f"    Error analyzing PR #{pr_number}: {e}")
            
            processed_repos += 1
        
        print(f"\n" + "=" * 80)
        print(f"ANALYSIS COMPLETE")
        print(f"Processed {processed_repos} repos, skipped {skipped_repos}")
        print(f"Total PRs analyzed: {len(all_pr_analysis)}")
        print("=" * 80)
        
        return all_pr_analysis
    
    def save_results(self, analysis_data):
        """Save analysis results to CSV"""
        if not analysis_data:
            print("No analysis data to save")
            return
        
        output_file = 'code_changes_analysis.csv'
        
        print(f"\nSaving detailed analysis to {output_file}...")
        
        fieldnames = [
            'repo_name', 'v1_commit', 'v2_commit', 'v1_date', 'v2_date',
            'pr_number', 'pr_title', 'pr_url', 'merge_date', 'author',
            'files_changed', 'api_additions', 'api_deletions',
            'files_added', 'files_modified', 'files_deleted',
            'total_lines_added', 'total_lines_removed',
            'code_additions', 'code_deletions',
            'comment_additions', 'comment_deletions',
            'languages_changed', 'change_types',
            'imports_added', 'functions_added', 'classes_added', 'test_changes'
        ]
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(analysis_data)
        
        print(f"Saved {len(analysis_data)} PR analyses")
    
    def generate_summary_statistics(self, analysis_data):
        """Generate summary statistics from analysis"""
        if not analysis_data:
            return None
        
        summary = {
            'total_prs_analyzed': len(analysis_data),
            'total_repos': len(set(pr['repo_name'] for pr in analysis_data)),
            'total_files_changed': sum(pr['files_changed'] for pr in analysis_data),
            'total_files_added': sum(pr['files_added'] for pr in analysis_data),
            'total_files_modified': sum(pr['files_modified'] for pr in analysis_data),
            'total_files_deleted': sum(pr['files_deleted'] for pr in analysis_data),
            'total_lines_added': sum(pr['total_lines_added'] for pr in analysis_data),
            'total_lines_removed': sum(pr['total_lines_removed'] for pr in analysis_data),
            'total_code_additions': sum(pr['code_additions'] for pr in analysis_data),
            'total_code_deletions': sum(pr['code_deletions'] for pr in analysis_data),
            'total_comment_additions': sum(pr['comment_additions'] for pr in analysis_data),
            'total_comment_deletions': sum(pr['comment_deletions'] for pr in analysis_data),
            'total_imports_added': sum(pr['imports_added'] for pr in analysis_data),
            'total_functions_added': sum(pr['functions_added'] for pr in analysis_data),
            'total_classes_added': sum(pr['classes_added'] for pr in analysis_data),
            'total_test_changes': sum(pr['test_changes'] for pr in analysis_data),
        }
        
        # Calculate averages
        summary['avg_files_per_pr'] = summary['total_files_changed'] / len(analysis_data)
        summary['avg_lines_added_per_pr'] = summary['total_lines_added'] / len(analysis_data)
        summary['avg_lines_removed_per_pr'] = summary['total_lines_removed'] / len(analysis_data)
        
        return summary
    
    def save_summary(self, summary_stats):
        """Save summary statistics"""
        if not summary_stats:
            return
        
        output_file = 'code_changes_summary.json'
        
        print(f"\nSaving summary statistics to {output_file}...")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(summary_stats, f, indent=2)
        
        print("\nSummary Statistics:")
        print("=" * 80)
        for key, value in summary_stats.items():
            if isinstance(value, float):
                print(f"{key}: {value:.2f}")
            else:
                print(f"{key}: {value}")
        print("=" * 80)


if __name__ == "__main__":
    analyzer = CodeChangeAnalyzer()
    analysis_results = analyzer.run_analysis()
    analyzer.save_results(analysis_results)
    summary = analyzer.generate_summary_statistics(analysis_results)
    analyzer.save_summary(summary)
    
    print("\nAnalysis complete! Results saved to:")
    print("  - code_changes_analysis.csv (detailed PR analysis)")
    print("  - code_changes_summary.json (summary statistics)")
