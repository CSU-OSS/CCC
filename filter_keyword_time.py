import os
import json
import time
import requests
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

# --- Configuration ---
COMMITS_FILE = "./output/ccs_commits.parquet"
ANALYSIS_CACHE_FILE = "./output/ccs_adoption_metadata.json"
COMMIT_DATE_FORMAT = "%d.%m.%Y %H:%M:%S"
CACHE_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
KEYWORD = "conventionalcommits.org"
MIN_REQUEST_INTERVAL = 0.5


class PrecisionCCSChecker:
    def __init__(self):
        self.github_token = os.getenv('GITHUB_TOKEN', '')
        if not self.github_token:
            raise ValueError("GITHUB_TOKEN not detected")

        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3.diff',
            'User-Agent': 'CCS-Precision-Checker/8.0'
        })
        self.last_request_time = 0

    def _wait(self):
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self.last_request_time = time.time()

    def _get_json(self, url: str, params: Optional[Dict] = None) -> Optional[Any]:
        while True:
            self._wait()
            headers = self.session.headers.copy()
            headers['Accept'] = 'application/vnd.github.v3+json'
            try:
                res = self.session.get(url, params=params, headers=headers, timeout=30)
                if res.status_code == 403:
                    reset = int(res.headers.get('X-RateLimit-Reset', time.time() + 60))
                    wait_time = max(reset - int(time.time()), 30)
                    print(f"\n   [Rate Limit] API limit reached, waiting {wait_time} seconds to retry...")
                    time.sleep(wait_time)
                    continue
                res.raise_for_status()
                return res.json()
            except Exception as e:
                print(f"\n   [Network Error] Request failed: {e} | Retrying in 30 seconds...")
                time.sleep(30)
                continue

    def _get_diff(self, commit_url: str) -> str:
        while True:
            self._wait()
            try:
                res = self.session.get(commit_url, timeout=45)
                res.raise_for_status()
                return res.text
            except Exception as e:
                print(f"\n   [Network Error] Diff retrieval failed: {e} | Retrying in 30 seconds...")
                time.sleep(30)
                continue

    def get_exact_adoption_date(self, repo_name: str) -> Optional[str]:
        print(f"   Querying repository: {repo_name}")
        search_url = f"https://api.github.com/search/code?q={KEYWORD}+repo:{repo_name}"
        search_res = self._get_json(search_url)

        if not search_res or not search_res.get('items'):
            return None

        matched_paths = [item['path'] for item in search_res['items']]
        found_dates = []

        for path in matched_paths:
            print(f"     Retrieving full file history: {path}")
            all_commits = []
            page = 1
            while True:
                params = {'path': path, 'per_page': 100, 'page': page}
                commits_url = f"https://api.github.com/repos/{repo_name}/commits"
                page_commits = self._get_json(commits_url, params=params)

                if not page_commits or len(page_commits) == 0:
                    break
                all_commits.extend(page_commits)
                if len(page_commits) < 100:
                    break
                page += 1

            introduced_found = False
            for commit_meta in reversed(all_commits):
                sha = commit_meta['sha']
                commit_url = f"https://api.github.com/repos/{repo_name}/commits/{sha}"
                diff_text = self._get_diff(commit_url)

                if any(line.startswith('+') and not line.startswith('+++') and KEYWORD in line for line in
                       diff_text.split('\n')):
                    date_str = commit_meta['commit']['author']['date']
                    dt = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')
                    found_dates.append(dt)
                    print(f"     Introduction point confirmed: {dt}")
                    introduced_found = True
                    break

            if not introduced_found and len(all_commits) > 0:
                oldest = all_commits[-1]
                date_str = oldest['commit']['author']['date']
                found_dates.append(datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ'))
                print(f"     Locking file creation point: {date_str}")

        if found_dates:
            return min(found_dates).strftime(CACHE_DATE_FORMAT)
        return None


class CommitDatasetProcessor:
    def __init__(self, checker: PrecisionCCSChecker):
        self.checker = checker
        self.repo_metadata: Dict[str, Dict] = {}

    def load_cache(self):
        path = Path(ANALYSIS_CACHE_FILE)
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                content = json.load(f)
                self.repo_metadata = content.get('repo_details', {})

    def save_cache(self):
        data = {
            'method': 'diff_deep_trace_v8_direct_cache',
            'last_update': datetime.now().strftime(CACHE_DATE_FORMAT),
            'repo_details': self.repo_metadata
        }
        with open(ANALYSIS_CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    def run(self):
        print("\n[Step 1] Loading raw data...")
        df = pd.read_parquet(COMMITS_FILE)
        df['repo'] = df['repo'].apply(lambda x: x[0] if isinstance(x, (list, np.ndarray)) else x).astype(str)
        df['commit_datetime'] = pd.to_datetime(df['date'], format=COMMIT_DATE_FORMAT, errors='coerce')
        df.dropna(subset=['commit_datetime', 'repo'], inplace=True)
        all_unique_repos = sorted(list(df['repo'].unique()))

        print(f"[Step 2] Confirming adoption dates (Total {len(all_unique_repos)} repositories)...")
        for repo in all_unique_repos:
            original_count = int(len(df[df['repo'] == repo]))

            if repo in self.repo_metadata and self.repo_metadata[repo].get('adoption_date'):
                print(f"   Using cache: {repo}")
            else:
                date_str = self.checker.get_exact_adoption_date(repo)
                self.repo_metadata[repo] = {
                    "adoption_date": date_str,
                    "original_count": original_count,
                    "kept_count": 0, "filtered_count": 0
                }
                self.save_cache()

        print("\n[Step 3] Executing filtering and statistics calculation...")
        final_dfs = []
        for repo in all_unique_repos:
            repo_df = df[df['repo'] == repo].copy()
            meta = self.repo_metadata.get(repo, {})
            adoption_date_str = meta.get('adoption_date')

            if adoption_date_str:
                adoption_dt = datetime.strptime(adoption_date_str, CACHE_DATE_FORMAT)
                kept_df = repo_df[repo_df['commit_datetime'] >= adoption_dt]
                meta['original_count'] = len(repo_df)
                meta['kept_count'] = len(kept_df)
                meta['filtered_count'] = len(repo_df) - len(kept_df)
                final_dfs.append(kept_df)
            else:
                meta['original_count'] = len(repo_df)
                meta['kept_count'] = len(repo_df)
                meta['filtered_count'] = 0
                final_dfs.append(repo_df)

        print("[Step 4] Overwriting and saving Parquet file...")
        if final_dfs:
            final_df = pd.concat(final_dfs, ignore_index=True)
            if 'commit_datetime' in final_df.columns:
                final_df = final_df.drop(columns=['commit_datetime'])

            final_df.to_parquet(COMMITS_FILE, index=False)
            self.save_cache()
            total_repos = len(all_unique_repos)
            print(f"Processing complete. Final retained records: {len(final_df)}")
            print(f"Total repositories: {total_repos}")

def main():
    try:
        checker = PrecisionCCSChecker()
        processor = CommitDatasetProcessor(checker)
        processor.load_cache()
        processor.run()
    except Exception as e:
        print(f"Program terminated: {e}")

if __name__ == "__main__":
    main()