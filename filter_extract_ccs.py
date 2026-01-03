"""
Filter and Extract Commits Dataset from High CCS-Compliance Repositories

This script processes datasets containing the 'is_CCS' field to perform the following:
1. Calculate the CCS compliance rate for each repository (ccs_rate = ccs_commits / total_commits).
2. Retain only repositories with a ccs_rate > 80%.
3. Extract only commits where is_CCS=1 from these high-compliance repositories.
4. Parse and extract 'type' and 'scope' fields from the commit messages.
5. Export the result to a new Parquet file.
"""

import re
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict


def parse_conventional_commit(message: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parses the commit message to extract type and scope.
    Supports standard format: type(scope)!: description
    """
    if not message or not isinstance(message, str):
        return None, None

    first_line = message.split('\n')[0].strip()

    # Pattern for simple format: type: description
    simple_pattern = r'^([a-zA-Z]+)!?:\s*(.+)'
    simple_match = re.match(simple_pattern, first_line)

    # Check if there is a scope indicated by parentheses
    type_with_scope_pattern = r'^([a-zA-Z]+)\('
    has_scope = re.match(type_with_scope_pattern, first_line)

    if has_scope:
        # Extract type and find matching closing parenthesis for the scope
        type_match = re.match(r'^([a-zA-Z]+)\(', first_line)
        if type_match:
            commit_type = type_match.group(1).lower()
            start_pos = len(commit_type) + 1

            # Counter for nested parentheses
            paren_count = 1
            scope_end = start_pos
            for i in range(start_pos, len(first_line)):
                if first_line[i] == '(':
                    paren_count += 1
                elif first_line[i] == ')':
                    paren_count -= 1
                    if paren_count == 0:
                        scope_end = i
                        break

            if paren_count == 0:
                scope = first_line[start_pos:scope_end]
                # Validate that the scope is followed by '!' or ':'
                remaining = first_line[scope_end + 1:].lstrip()
                if remaining.startswith('!:') or remaining.startswith(':'):
                    return commit_type, scope if scope else None

    elif simple_match:
        commit_type = simple_match.group(1).lower()
        return commit_type, None

    return None, None


def calculate_repo_ccs_rates(df: pd.DataFrame) -> Dict[str, Dict]:
    print("\nCalculating CCS compliance rates per repository...")

    repo_stats = {}
    repo_groups = df.groupby('repo')

    for repo_name, group in repo_groups:
        total_commits = len(group)
        ccs_commits = group['is_CCS'].sum()
        non_ccs_commits = total_commits - ccs_commits
        ccs_rate = float(ccs_commits / total_commits) if total_commits > 0 else 0.0

        repo_stats[repo_name] = {
            'total_commits': int(total_commits),
            'ccs_commits': int(ccs_commits),
            'non_ccs_commits': int(non_ccs_commits),
            'ccs_rate': ccs_rate
        }

    return repo_stats


def filter_high_ccs_rate_repos(
        df: pd.DataFrame,
        repo_stats: Dict[str, Dict],
        min_ccs_rate: float = 0.8
) -> pd.DataFrame:
    print(f"\nFiltering repositories with ccs_rate > {min_ccs_rate * 100:.0f}%...")

    high_rate_repos = [
        repo for repo, stats in repo_stats.items()
        if stats['ccs_rate'] > min_ccs_rate
    ]

    print(f"Qualifying repositories: {len(high_rate_repos)} / {len(repo_stats)}")

    # Filter for qualifying repos AND only keep valid CCS commits
    filtered_df = df[(df['repo'].isin(high_rate_repos)) & (df['is_CCS'] == 1)].copy()

    return filtered_df


def print_repo_statistics(
        repo_stats: Dict[str, Dict],
        min_ccs_rate: float = 0.8,
        top_n: int = 10
) -> None:
    print("\n" + "=" * 80)
    print("Repository CCS Compliance Statistics")
    print("=" * 80)

    total_repos = len(repo_stats)
    high_rate_repos = {k: v for k, v in repo_stats.items() if v['ccs_rate'] > min_ccs_rate}
    low_rate_repos = {k: v for k, v in repo_stats.items() if v['ccs_rate'] <= min_ccs_rate}

    print(f"\nTotal Repositories: {total_repos:,}")
    print(
        f"Repos with ccs_rate > {min_ccs_rate * 100:.0f}%: {len(high_rate_repos):,} ({len(high_rate_repos) / total_repos * 100:.2f}%)")
    print(
        f"Repos with ccs_rate ≤ {min_ccs_rate * 100:.0f}%: {len(low_rate_repos):,} ({len(low_rate_repos) / total_repos * 100:.2f}%)")

    if high_rate_repos:
        print(f"\nTop {min(top_n, len(high_rate_repos))} high-compliance repositories:")
        print("-" * 80)
        sorted_repos = sorted(
            high_rate_repos.items(),
            key=lambda x: x[1]['ccs_rate'],
            reverse=True
        )[:top_n]

        for i, (repo, stats) in enumerate(sorted_repos, 1):
            print(f"  [{i}] {repo}")
            print(f"      Total: {stats['total_commits']}, "
                  f"CCS: {stats['ccs_commits']}, "
                  f"Non-CCS: {stats['non_ccs_commits']}, "
                  f"Rate: {stats['ccs_rate'] * 100:.2f}%")

    print("=" * 80)


def filter_and_extract_high_rate_commits(
        input_file: str,
        output_file: str,
        min_ccs_rate: float = 0.8,
        batch_size: int = 10000,
        save_analysis: bool = True
):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        print(f"Error: Input file not found: {input_file}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print(f"High-Compliance Dataset Extraction (ccs_rate > {min_ccs_rate * 100:.0f}%)")
    print("=" * 80)
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print(f"Threshold: > {min_ccs_rate * 100:.0f}%")
    print("=" * 80)

    print("\nReading parquet file...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"Total records loaded: {total_records:,}")

    if 'is_CCS' not in df.columns or 'repo' not in df.columns:
        print(f"Error: Missing required fields ('is_CCS' or 'repo')")
        print("Available fields:", df.columns.tolist())
        return

    repo_stats = calculate_repo_ccs_rates(df)
    print_repo_statistics(repo_stats, min_ccs_rate, top_n=10)

    filtered_df = filter_high_ccs_rate_repos(df, repo_stats, min_ccs_rate)

    print(f"\nFiltered record count: {len(filtered_df):,}")
    print(f"Retention rate: {len(filtered_df) / total_records * 100:.2f}%")

    if len(filtered_df) == 0:
        print("Warning: No records met the criteria. Output not generated.")
        return

    print(f"\nExtracting commit type and scope...")

    type_list = []
    scope_list = []

    # Process in batches for logging
    for idx, message in enumerate(filtered_df["message"]):
        if (idx + 1) % batch_size == 0 or (idx + 1) == len(filtered_df):
            print(f"Processed: {idx + 1:,}/{len(filtered_df):,} ({(idx + 1) / len(filtered_df) * 100:.1f}%)")

        commit_type, scope = parse_conventional_commit(message)
        type_list.append(commit_type)
        scope_list.append(scope)

    filtered_df['commit_type'] = type_list
    filtered_df['commit_scope'] = scope_list

    print(f"\nSaving results to: {output_file}")
    filtered_df.to_parquet(output_file, index=False)

    if save_analysis:
        analysis_file = output_path.parent / f"{output_path.stem}_analysis.json"
        save_repo_analysis(repo_stats, filtered_df, min_ccs_rate, str(analysis_file))

    print("\n" + "=" * 80)
    print("Processing Complete")
    print("=" * 80)
    print(f"Final Count: {len(filtered_df):,}")
    print(f"Unique Repos: {filtered_df['repo'].nunique():,}")
    print(f"Fields added: commit_type, commit_scope")
    print("=" * 80)


def save_repo_analysis(
        repo_stats: Dict[str, Dict],
        filtered_df: pd.DataFrame,
        min_ccs_rate: float,
        output_file: str
) -> None:
    import json
    from datetime import datetime

    high_rate_repos = {k: v for k, v in repo_stats.items() if v['ccs_rate'] > min_ccs_rate}

    analysis_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'filter_criteria': {
            'min_ccs_rate': min_ccs_rate,
            'description': f'Retained repositories with ccs_rate > {min_ccs_rate * 100:.0f}%'
        },
        'statistics': {
            'total_repos': len(repo_stats),
            'filtered_repos': len(high_rate_repos),
            'removed_repos': len(repo_stats) - len(high_rate_repos),
            'total_commits': len(filtered_df),
            'ccs_commits_extracted': int(len(filtered_df))
        },
        'high_rate_repos': high_rate_repos
    }

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis_data, f, ensure_ascii=False, indent=2)

    print(f"\nAnalysis metadata saved to: {output_file}")

def main():
    # Use standard file paths for a research workflow
    input_file = "./output/commits_true_ccs_repos.parquet"
    output_file = "./output/ccs_commits.parquet"

    filter_and_extract_high_rate_commits(
        input_file=input_file,
        output_file=output_file,
        min_ccs_rate=0.8,
        batch_size=10000,
        save_analysis=True
    )

if __name__ == "__main__":
    main()