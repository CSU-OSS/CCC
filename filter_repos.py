"""
Filter Truly CCS-Compliant Repositories

This script reads a dataset containing the 'is_CCS' field and filters out
all repositories where the 'is_CCS' value is 0 for every single commit
(i.e., repositories that claim to use CCS but have no valid CCS commits).
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List


class TrueCCSRepoFilter:
    def __init__(self):
        self.stats = {
            'total_records': 0,
            'total_repos': 0,
            'true_ccs_repos': 0,
            'false_ccs_repos': 0,
            'filtered_records': 0,
            'removed_records': 0
        }
        self.repo_ccs_status: Dict[str, Dict] = {}

    def analyze_repos(self, df: pd.DataFrame) -> None:
        print("\nAnalyzing CCS compliance across repositories...")

        repo_groups = df.groupby('repo')

        for repo_name, group in repo_groups:
            total_commits = len(group)
            ccs_commits = group['is_CCS'].sum()
            non_ccs_commits = total_commits - ccs_commits

            # A repository is considered "True CCS" if it has at least one valid CCS commit
            is_true_ccs = ccs_commits > 0

            self.repo_ccs_status[repo_name] = {
                'total_commits': int(total_commits),
                'ccs_commits': int(ccs_commits),
                'non_ccs_commits': int(non_ccs_commits),
                'ccs_rate': float(ccs_commits / total_commits) if total_commits > 0 else 0.0,
                'is_true_ccs': bool(is_true_ccs)
            }

            if is_true_ccs:
                self.stats['true_ccs_repos'] += 1
            else:
                self.stats['false_ccs_repos'] += 1

        self.stats['total_repos'] = len(self.repo_ccs_status)

    def filter_dataset(self, df: pd.DataFrame) -> pd.DataFrame:
        print("\nFiltering dataset...")

        true_ccs_repos = [
            repo for repo, status in self.repo_ccs_status.items()
            if status['is_true_ccs']
        ]

        filtered_df = df[df['repo'].isin(true_ccs_repos)].copy()

        self.stats['total_records'] = len(df)
        self.stats['filtered_records'] = len(filtered_df)
        self.stats['removed_records'] = self.stats['total_records'] - self.stats['filtered_records']

        return filtered_df

    def print_repo_analysis(self, top_n: int = 10) -> None:
        print("\n" + "=" * 80)
        print("Repository CCS Compliance Analysis")
        print("=" * 80)

        true_ccs_repos = {k: v for k, v in self.repo_ccs_status.items() if v['is_true_ccs']}
        false_ccs_repos = {k: v for k, v in self.repo_ccs_status.items() if not v['is_true_ccs']}

        print(f"\nTotal Repositories: {self.stats['total_repos']:,}")
        print(
            f"True CCS Repositories: {self.stats['true_ccs_repos']:,} ({self.stats['true_ccs_repos'] / self.stats['total_repos'] * 100:.2f}%)")
        print(
            f"False CCS Repositories: {self.stats['false_ccs_repos']:,} ({self.stats['false_ccs_repos'] / self.stats['total_repos'] * 100:.2f}%)")

        if false_ccs_repos:
            print(f"\nList of False CCS Repositories (All commits have is_CCS=0):")
            print("-" * 80)
            for i, (repo, status) in enumerate(false_ccs_repos.items(), 1):
                print(f"  [{i}] {repo} (Total Commits: {status['total_commits']})")

        if true_ccs_repos:
            print(f"\nTop {min(top_n, len(true_ccs_repos))} Repositories by CCS Compliance Rate:")
            print("-" * 80)
            sorted_repos = sorted(
                true_ccs_repos.items(),
                key=lambda x: x[1]['ccs_rate'],
                reverse=True
            )[:top_n]

            for i, (repo, status) in enumerate(sorted_repos, 1):
                print(f"  [{i}] {repo}")
                print(f"      Total Commits: {status['total_commits']}, "
                      f"CCS Compliant: {status['ccs_commits']}, "
                      f"Non-compliant: {status['non_ccs_commits']}, "
                      f"Compliance Rate: {status['ccs_rate'] * 100:.2f}%")

        print("=" * 80)

    def print_final_stats(self) -> None:
        print("\n" + "=" * 80)
        print("Final Filtering Statistics")
        print("=" * 80)
        print(f"Original Record Count: {self.stats['total_records']:,}")
        print(f"Filtered Record Count: {self.stats['filtered_records']:,}")
        print(f"Removed Record Count:  {self.stats['removed_records']:,}")
        print(f"Record Retention Rate: {self.stats['filtered_records'] / self.stats['total_records'] * 100:.2f}%")
        print("-" * 80)
        print(f"Original Repo Count:   {self.stats['total_repos']:,}")
        print(f"Retained Repo Count:   {self.stats['true_ccs_repos']:,}")
        print(f"Removed Repo Count:    {self.stats['false_ccs_repos']:,}")
        print(f"Repo Retention Rate:   {self.stats['true_ccs_repos'] / self.stats['total_repos'] * 100:.2f}%")
        print("=" * 80)

    def save_repo_analysis(self, output_file: str) -> None:
        import json
        from datetime import datetime

        analysis_data = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'statistics': self.stats,
            'repo_details': self.repo_ccs_status
        }

        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_data, f, ensure_ascii=False, indent=2)

        print(f"\nRepository analysis results saved to: {output_file}")


def filter_true_ccs_repos(
        input_file: str,
        output_file: str,
        analysis_file: str = None
):
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_file}")
        return

    print("=" * 80)
    print("Filtering Truly CCS-Compliant Repositories")
    print("=" * 80)
    print(f"Input file:    {input_file}")
    print(f"Output file:   {output_file}")
    if analysis_file:
        print(f"Analysis file: {analysis_file}")
    print("=" * 80)

    print("\nReading dataset...")
    df = pd.read_parquet(input_file)
    print(f"Read complete. Found {len(df):,} records.")

    if 'is_CCS' not in df.columns:
        print("Error: 'is_CCS' field missing in dataset!")
        print("Please run the 'add_is_ccs.py' script first to generate the compliance field.")
        return

    if 'repo' not in df.columns:
        print("Error: 'repo' field missing in dataset!")
        return

    filter_obj = TrueCCSRepoFilter()
    filter_obj.analyze_repos(df)

    filter_obj.print_repo_analysis(top_n=10)

    filtered_df = filter_obj.filter_dataset(df)

    print(f"\nSaving filtered data to: {output_file}")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_parquet(output_file, index=False)

    if analysis_file:
        filter_obj.save_repo_analysis(analysis_file)

    filter_obj.print_final_stats()

    print(f"\nFiltering complete! Results saved to: {output_file}")

def main():
    input_file = "./output/commits_by_repo.parquet"
    output_file = "./output/commits_true_ccs_repos.parquet"
    analysis_file = "./output/repo_ccs_analysis.json"

    filter_true_ccs_repos(
        input_file=input_file,
        output_file=output_file,
        analysis_file=analysis_file
    )

if __name__ == "__main__":
    main()