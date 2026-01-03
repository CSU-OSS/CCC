"""
Keyword-based CCS Compliance Checker and Dataset Processor

This script contains two main functionalities:
1. KeywordCCSChecker: Determines if a repository adopts CCS standards by searching for the "conventionalcommits.org" keyword.
2. CommitDatasetProcessorByKeyword: Processes the commit-chronicle dataset to filter CCS-compliant repos and commits.
"""

import os
import sys
import re
import json
import time
import requests
import pandas as pd
import pyarrow.parquet as pq
from typing import Dict, List, Optional, Set, Any
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class KeywordCCSChecker:

    def __init__(self, github_token: Optional[str] = None):
        self.github_token = github_token or os.getenv('GITHUB_TOKEN', '')
        if not self.github_token:
            raise ValueError(
                "GitHub token is required. Please set GITHUB_TOKEN environment variable "
                "or create a .env file with GITHUB_TOKEN=your_token"
            )
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {self.github_token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'CCS-Keyword-Checker/2.0'
        })

        self.keyword = "conventionalcommits.org"

        self.last_request_time = 0
        self.min_request_interval = 1.0

    def _wait_for_rate_limit(self):
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_request_interval:
            time.sleep(self.min_request_interval - elapsed)
        self.last_request_time = time.time()

    def _make_github_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        self._wait_for_rate_limit()

        try:
            response = self.session.get(url, params=params)

            if response.status_code == 403:
                reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                current_time = int(time.time())
                if reset_time > current_time:
                    wait_time = reset_time - current_time + 1
                    print(f"API rate limit reached. Waiting for {wait_time} seconds...")
                    time.sleep(wait_time)
                    return self._make_github_request(url, params)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                print(f"GitHub API request failed: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            print(f"Network request error: {e}")
            return None

    def search_keyword_in_repo(self, repo_name: str) -> bool:
        try:
            search_url = "https://api.github.com/search/code"
            params = {
                'q': f'{self.keyword} repo:{repo_name}',
                'per_page': 1
            }

            print(f"Searching keyword '{self.keyword}' in repository: {repo_name}")

            result = self._make_github_request(search_url, params)

            if result is None:
                print(f"Unable to search repository: {repo_name}")
                return False

            total_count = result.get('total_count', 0)
            found = total_count > 0

            if found:
                print(f"Keyword '{self.keyword}' found in {repo_name} ({total_count} times)")

                items = result.get('items', [])
                if items:
                    first_item = items[0]
                    file_path = first_item.get('path', 'unknown')
                    print(f"    First appearance in file: {file_path}")
            else:
                print(f"Keyword '{self.keyword}' not found in {repo_name}")

            return found

        except Exception as e:
            print(f"Error searching repository {repo_name}: {e}")
            return False

    def check_repository(self, repo_name: str, verbose: bool = True) -> bool:
        if verbose:
            print(f"Checking repository: {repo_name}")
            print(f"Method: Searching for keyword '{self.keyword}'")
            print("-" * 60)

        repo_url = f"https://api.github.com/repos/{repo_name}"
        repo_info = self._make_github_request(repo_url)

        if repo_info is None:
            if verbose:
                print(f"Repository {repo_name} does not exist or is inaccessible")
            return False

        has_keyword = self.search_keyword_in_repo(repo_name)

        if verbose:
            result_text = "CCS Compliant" if has_keyword else "Non-CCS Compliant"
            print(f"Result: {result_text}")
            print("-" * 60)

        return has_keyword

    def batch_check(self, repo_names: list, verbose: bool = False) -> Dict[str, bool]:
        results = {}
        total = len(repo_names)

        print(f"Starting batch check for {total} repositories...")
        print(f"Method: Keyword search for '{self.keyword}'")
        print("=" * 80)

        conventional_count = 0

        for i, repo_name in enumerate(repo_names, 1):
            print(f"\n[{i}/{total}] Checking: {repo_name}")

            try:
                is_conventional = self.check_repository(repo_name, verbose=verbose)
                results[repo_name] = is_conventional

                if is_conventional:
                    conventional_count += 1

                status = "[OK]" if is_conventional else "[NO]"
                print(f"         Status: {status}")

            except Exception as e:
                results[repo_name] = False
                print(f"         Status: [NO] (Error: {e})")

        print(f"\nBatch Check Summary")
        print("=" * 40)
        print(f"Total Repositories: {total}")
        print(f"CCS Compliant: {conventional_count} ({conventional_count / total:.1%})")
        print(f"Non-CCS Compliant: {total - conventional_count}")
        print("=" * 40)

        return results

    def save_results(self, results: Dict[str, bool], output_file: str):
        try:
            output_data = {
                'method': 'keyword_search',
                'keyword': self.keyword,
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_repos': len(results),
                'conventional_repos': sum(results.values()),
                'results': results
            }

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)

            print(f"Results saved to: {output_file}")

        except Exception as e:
            print(f"Failed to save results: {e}")

    def load_results(self, input_file: str) -> Optional[Dict[str, bool]]:
        try:
            if not Path(input_file).exists():
                print(f"Result file not found: {input_file}")
                return None

            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            results = data.get('results', {})
            print(f"Loaded check results for {len(results)} repositories")

            return results

        except Exception as e:
            print(f"Failed to load results: {e}")
            return None


class CommitDatasetProcessorByKeyword:
    def __init__(self, github_token: Optional[str] = None):
        self.github_token = github_token or os.getenv('GITHUB_TOKEN', '')

        self.repo_checker = KeywordCCSChecker(github_token=self.github_token)

        self.repo_cache: Dict[str, bool] = {}

        self.stats = {
            'total_records': 0,
            'unique_repos': 0,
            'conventional_repos': 0,
            'processed_records': 0,
            'skipped_records': 0
        }

    def is_conventional_repo(self, repo_name: str) -> bool:
        if repo_name in self.repo_cache:
            return self.repo_cache[repo_name]

        try:
            print(f"Checking repository: {repo_name}")

            is_conventional = self.repo_checker.check_repository(
                repo_name,
                verbose=False
            )

            self.repo_cache[repo_name] = is_conventional

            status = "[OK]" if is_conventional else "[NO]"
            method_desc = f"Search '{self.repo_checker.keyword}'"
            print(
                f"    Result: {status} {'Confirmed' if is_conventional else 'Not confirmed'} CCS compliance ({method_desc})")

            return is_conventional

        except Exception as e:
            print(f"Error checking repository {repo_name}: {e}")
            self.repo_cache[repo_name] = False
            return False

    def extract_diff_content(self, mods) -> str:
        if mods is None:
            return ""

        if hasattr(mods, 'tolist'):
            mods = mods.tolist()

        if not isinstance(mods, list):
            return ""

        diff_parts = []
        for mod in mods:
            if isinstance(mod, dict) and 'diff' in mod:
                diff_content = mod.get('diff', '')
                if diff_content:
                    diff_parts.append(diff_content)

        return '\n---\n'.join(diff_parts)

    def safe_extract(self, value):
        if value is None:
            return None

        if hasattr(value, 'iloc'):
            return value.iloc[0] if len(value) > 0 else None

        if hasattr(value, 'item'):
            try:
                return value.item()
            except ValueError:
                pass

        if hasattr(value, 'size') and hasattr(value, 'flatten'):
            flat_value = value.flatten()
            if flat_value.size > 0:
                try:
                    return flat_value[0]
                except (IndexError, ValueError):
                    return None
            else:
                return None

        return value

    def is_valid_string(self, value):
        if value is None:
            return False

        if isinstance(value, str):
            return bool(value.strip())

        return False

    def process_single_record(self, record: Dict) -> Optional[Dict]:
        try:
            repo_name = self.safe_extract(record.get('repo'))
            message = self.safe_extract(record.get('message')) or self.safe_extract(record.get('original_message', ''))

            if not self.is_valid_string(repo_name) or not self.is_valid_string(message):
                self.stats['skipped_records'] += 1
                return None

            if not self.is_conventional_repo(repo_name):
                self.stats['skipped_records'] += 1
                return None

            self.stats['processed_records'] += 1

            return record

        except Exception as e:
            print(f"Error processing record: {e}")
            self.stats['skipped_records'] += 1
            return None

    def process_batch(self, records: List[Dict]) -> List[Dict]:
        processed_records = []

        for i, record in enumerate(records):
            if i % 100 == 0:
                print(f"Processed {i}/{len(records)} records...")

            processed_record = self.process_single_record(record)
            if processed_record:
                processed_records.append(processed_record)

        return processed_records

    def print_final_stats(self) -> None:
        print("\n" + "=" * 80)
        print("Final Statistical Report (Keyword-based Method)")
        print("=" * 80)
        print(f"Check Method: Keyword search for '{self.repo_checker.keyword}'")
        print(f"Total Records: {self.stats['total_records']:,}")
        print(f"Unique Repositories: {self.stats['unique_repos']:,}")
        print(f"CCS Compliant Repositories: {self.stats['conventional_repos']:,}")
        print(f"Retained Commit Records: {self.stats['processed_records']:,}")
        print(f"Skipped Records: {self.stats['skipped_records']:,}")

        if len(self.repo_cache) > 0:
            repo_ccs_rate = self.stats['conventional_repos'] / len(self.repo_cache) * 100
            print(f"Repository CCS Adoption Rate: {repo_ccs_rate:.1f}%")

        print("=" * 80)

    def save_repo_cache(self, cache_file: str) -> None:
        try:
            cache_data = {
                'method': 'keyword_search',
                'keyword': self.repo_checker.keyword,
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_repos': len(self.repo_cache),
                'conventional_repos': sum(1 for v in self.repo_cache.values() if v),
                'cache': self.repo_cache
            }

            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
            print(f"Repo cache saved to: {cache_file}")
        except Exception as e:
            print(f"Failed to save cache: {e}")

    def load_repo_cache(self, cache_file: str) -> None:
        try:
            if os.path.exists(cache_file):
                with open(cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if 'cache' in data:
                    self.repo_cache = data['cache']
                    method = data.get('method', 'unknown')
                    keyword = data.get('keyword', 'unknown')
                    print(
                        f"Repo cache loaded: {len(self.repo_cache)} repositories (Method: {method}, Keyword: {keyword})")
                else:
                    self.repo_cache = data
                    print(f"Repo cache loaded: {len(self.repo_cache)} repositories (Legacy format)")
            else:
                print("Cache file does not exist. Creating new cache...")
        except Exception as e:
            print(f"Failed to load cache: {e}")
            self.repo_cache = {}


def test_checker():
    print("Testing Keyword-based CCS Compliance Checker")
    print("=" * 80)

    # Test repository list
    test_repos = [
        "microsoft/vscode",
        "facebook/react",
        "angular/angular",
        "vuejs/vue",
        "nodejs/node"
    ]

    checker = KeywordCCSChecker()

    results = checker.batch_check(test_repos, verbose=True)

    output_file = "test_keyword_results.json"
    checker.save_results(results, output_file)


def process_dataset():
    print("Keyword-based Commit Chronicle Dataset Processor")
    print("=" * 80)

    input_dir = Path("./commit-chronicle-data/data")
    output_dir = Path("./output")
    output_file = output_dir / "commits_by_repo.parquet"
    cache_file = output_dir / "repo_cache_keyword.json"

    output_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = sorted(input_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"No parquet files found in directory: {input_dir}")
        return

    print(f"Found {len(parquet_files)} parquet files")
    for i, f in enumerate(parquet_files, 1):
        print(f"  [{i}] {f.name}")
    print(f"Output file: {output_file}")
    print("=" * 80)

    processor = CommitDatasetProcessorByKeyword()

    processor.load_repo_cache(str(cache_file))

    all_processed_records = []

    try:
        for idx, input_file in enumerate(parquet_files, 1):
            print(f"\n{'=' * 80}")
            print(f"Processing file [{idx}/{len(parquet_files)}]: {input_file.name}")
            print(f"{'=' * 80}")

            try:
                print("Reading parquet file...")
                df = pd.read_parquet(input_file)

                file_total_records = len(df)
                print(f"Records in file: {file_total_records:,}")

                unique_repos = df['repo'].nunique()
                print(f"Unique repositories: {unique_repos:,}")

                records = df.to_dict('records')

                chunk_size = 100
                total_batches = (len(records) + chunk_size - 1) // chunk_size

                for i in range(0, len(records), chunk_size):
                    batch_num = i // chunk_size + 1
                    if batch_num % 10 == 0 or batch_num == 1:
                        print(f"Processing batch {batch_num}/{total_batches}")

                    batch_records = records[i:i + chunk_size]
                    processed_batch = processor.process_batch(batch_records)
                    all_processed_records.extend(processed_batch)

                print(f"File {input_file.name} processed successfully")
                print(
                    f"   Valid records from this file: {len(all_processed_records) - len([r for r in all_processed_records if r])}")

                processor.save_repo_cache(str(cache_file))

            except Exception as e:
                print(f"Error processing file {input_file.name}: {e}")
                import traceback
                traceback.print_exc()
                continue

        if not all_processed_records:
            print("\nWARNING: No compliant records found!")
            return

        print(f"\n{'=' * 80}")
        print(f"Saving all results to {output_file}...")
        result_df = pd.DataFrame(all_processed_records)
        result_df.to_parquet(output_file, index=False)

        processor.stats['conventional_repos'] = sum(1 for v in processor.repo_cache.values() if v)

        processor.print_final_stats()

        print(f"\n{'=' * 80}")
        print(f"All files processed!")
        print(f"Files handled: {len(parquet_files)}")
        print(f"Total valid records: {len(all_processed_records):,}")
        print(f"Output file: {output_file}")
        print(f"{'=' * 80}")

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")

        processor.save_repo_cache(str(cache_file))
        if all_processed_records:
            print(f"Saving {len(all_processed_records):,} processed records...")
            result_df = pd.DataFrame(all_processed_records)
            result_df.to_parquet(output_file, index=False)
            print(f"Partial results saved to: {output_file}")
    except Exception as e:
        print(f"Runtime error: {e}")
        import traceback
        traceback.print_exc()

def main():
    process_dataset()

if __name__ == "__main__":
    main()