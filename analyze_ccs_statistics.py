"""
CCS Commits Dataset Statistical Analysis Script

This script reads a Parquet file containing language, repo, and message fields,
and generates a detailed statistical report, including:
1. Programming language types and counts across different repositories
2. Programming language types and counts per commit
3. Types and counts of commit 'types'
4. Types and counts of commit 'scopes'
"""

import pandas as pd
from pathlib import Path
from collections import Counter
import sys


def analyze_ccs_statistics(
        input_file: str,
        output_dir: str = None,
        top_n: int = 50
):
    input_path = Path(input_file)

    if not input_path.exists():
        print(f"Error: Input file does not exist: {input_file}")
        return

    if output_dir is None:
        output_dir = input_path.parent
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 80)
    print("CCS Commits Dataset - Statistical Analysis")
    print("=" * 80)
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    print("=" * 80)

    print("\nReading parquet file...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"Total records: {total_records:,}")

    required_fields = ['language', 'repo', 'commit_type', 'commit_scope']
    missing_fields = [f for f in required_fields if f not in df.columns]

    if missing_fields:
        print(f"Error: Missing required fields: {missing_fields}")
        print(f"Available fields: {df.columns.tolist()}")
        return

    # Analysis 1: Programming language distribution by repository
    print("\n" + "=" * 80)
    print("Analysis 1: Language distribution across repositories")
    print("=" * 80)

    repo_language_df = df[['repo', 'language']].drop_duplicates()
    repo_lang_counts = repo_language_df['language'].value_counts()

    total_repos = df['repo'].nunique()
    unique_languages_repos = len(repo_lang_counts)

    print(f"Total CCS-compliant repositories: {total_repos:,}")
    print(f"Number of unique languages: {unique_languages_repos}")

    print(f"\nLanguage distribution (by repository count):")
    print(f"{'Language':<20} {'Repo Count':<15}")
    print("-" * 40)
    for lang, count in repo_lang_counts.items():
        lang_display = str(lang) if pd.notna(lang) else 'None'
        print(f"{lang_display:<20} {count:<15}")

    # Analysis 2: Programming language distribution by commit
    print("\n" + "=" * 80)
    print("Analysis 2: Language distribution across commits")
    print("=" * 80)

    commit_lang_counts = df['language'].value_counts()
    unique_languages_commits = len(commit_lang_counts)

    print(f"Total CCS-compliant commits: {total_records:,}")
    print(f"Number of unique languages: {unique_languages_commits}")

    print(f"\nLanguage distribution (by commit count):")
    print(f"{'Language':<20} {'Commit Count':<15}")
    print("-" * 40)
    for lang, count in commit_lang_counts.items():
        lang_display = str(lang) if pd.notna(lang) else 'None'
        print(f"{lang_display:<20} {count:<15}")

    # Analysis 3: Distribution of commit 'types'
    print("\n" + "=" * 80)
    print("Analysis 3: Distribution of commit 'types'")
    print("=" * 80)

    type_series = df['commit_type'].fillna('None')
    type_counts = type_series.value_counts()

    valid_type_count = (df['commit_type'].notna()).sum()
    none_type_count = (df['commit_type'].isna()).sum()

    print(f"Records with valid type: {valid_type_count:,} ({valid_type_count / total_records * 100:.2f}%)")
    print(f"Records with invalid type: {none_type_count:,} ({none_type_count / total_records * 100:.2f}%)")
    print(f"Number of unique types: {len(type_counts)}")

    print(f"\nType Distribution (Top {min(top_n, len(type_counts))}):")
    print(f"{'Type':<20} {'Commit Count':<15}")
    print("-" * 40)
    for commit_type, count in type_counts.head(top_n).items():
        print(f"{commit_type:<20} {count:<15}")

    # Analysis 4: Distribution of commit 'scopes'
    print("\n" + "=" * 80)
    print("Analysis 4: Distribution of commit 'scopes'")
    print("=" * 80)

    scope_series = df['commit_scope'].fillna('None')
    scope_counts = scope_series.value_counts()

    valid_scope_count = (df['commit_scope'].notna()).sum()
    none_scope_count = (df['commit_scope'].isna()).sum()

    print(f"Records with scope: {valid_scope_count:,} ({valid_scope_count / total_records * 100:.2f}%)")
    print(f"Records without scope: {none_scope_count:,} ({none_scope_count / total_records * 100:.2f}%)")
    print(f"Number of unique scopes: {len(scope_counts)}")

    print(f"\nScope Distribution (Top {min(top_n, len(scope_counts))}):")
    print(f"{'Scope':<40} {'Commit Count':<15}")
    print("-" * 60)
    for scope, count in scope_counts.head(top_n).items():
        scope_display = str(scope)[:40]
        print(f"{scope_display:<40} {count:<15}")

    # Save statistics report to text file
    print("\nSaving statistical results...")
    txt_output = output_dir / "ccs_statistics_report.txt"
    save_text_report(txt_output, df, repo_lang_counts, commit_lang_counts, type_counts, scope_counts, top_n)

    # Save statistics to CSV files
    repo_lang_csv = output_dir / "repo_language_statistics.csv"
    save_repo_language_to_csv(repo_lang_csv, repo_lang_counts)

    commit_lang_csv = output_dir / "commit_language_statistics.csv"
    save_commit_language_to_csv(commit_lang_csv, commit_lang_counts)

    type_csv = output_dir / "commit_type_statistics.csv"
    save_type_to_csv(type_csv, type_counts, total_records)

    scope_csv = output_dir / "commit_scope_statistics.csv"
    save_scope_to_csv(scope_csv, scope_counts, total_records)

    print("\n" + "=" * 80)
    print("Statistical Analysis Completed")
    print("=" * 80)
    print(f"Text Report: {txt_output}")
    print(f"Repo Language CSV: {repo_lang_csv}")
    print(f"Commit Language CSV: {commit_lang_csv}")
    print(f"Type Statistics CSV: {type_csv}")
    print(f"Scope Statistics CSV: {scope_csv}")
    print("=" * 80)


def save_text_report(output_file, df, repo_lang_counts, commit_lang_counts, type_counts, scope_counts, top_n):
    total_records = len(df)
    total_repos = df['repo'].nunique()

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("CCS Commits Dataset Statistical Report\n")
        f.write("=" * 80 + "\n\n")

        # Analysis 1: Repo Language Stats
        f.write("Analysis 1: Language distribution across repositories\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total CCS-compliant repositories: {total_repos:,}\n")
        f.write(f"Number of unique languages: {len(repo_lang_counts)}\n\n")

        f.write(f"{'Language':<20} {'Repo Count':<15}\n")
        f.write("-" * 40 + "\n")
        for lang, count in repo_lang_counts.items():
            lang_display = str(lang) if pd.notna(lang) else 'None'
            f.write(f"{lang_display:<20} {count:<15}\n")

        # Analysis 2: Commit Language Stats
        f.write("\n\nAnalysis 2: Language distribution across commits\n")
        f.write("-" * 80 + "\n")
        f.write(f"Total CCS-compliant commits: {total_records:,}\n")
        f.write(f"Number of unique languages: {len(commit_lang_counts)}\n\n")

        f.write(f"{'Language':<20} {'Commit Count':<15}\n")
        f.write("-" * 40 + "\n")
        for lang, count in commit_lang_counts.items():
            lang_display = str(lang) if pd.notna(lang) else 'None'
            f.write(f"{lang_display:<20} {count:<15}\n")

        # Analysis 3: Type Stats
        f.write("\n\nAnalysis 3: Distribution of commit 'types'\n")
        f.write("-" * 80 + "\n")
        f.write(f"Number of unique types: {len(type_counts)}\n\n")

        f.write(f"{'Type':<20} {'Commit Count':<15}\n")
        f.write("-" * 40 + "\n")
        for commit_type, count in type_counts.items():
            f.write(f"{commit_type:<20} {count:<15}\n")

        # Analysis 4: Scope Stats
        f.write("\n\nAnalysis 4: Distribution of commit 'scopes'\n")
        f.write("-" * 80 + "\n")
        f.write(f"Number of unique scopes: {len(scope_counts)}\n\n")

        f.write(f"Scope Distribution (Top {min(top_n, len(scope_counts))}):\n")
        f.write(f"{'Scope':<40} {'Commit Count':<15}\n")
        f.write("-" * 60 + "\n")
        for scope, count in scope_counts.head(top_n).items():
            f.write(f"{str(scope):<40} {count:<15}\n")

    print(f"Text report saved: {output_file}")


def save_repo_language_to_csv(output_file, repo_lang_counts):
    result_df = pd.DataFrame({
        'language': repo_lang_counts.index,
        'repo_count': repo_lang_counts.values
    })

    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Repo language statistics CSV saved: {output_file}")


def save_commit_language_to_csv(output_file, commit_lang_counts):
    result_df = pd.DataFrame({
        'language': commit_lang_counts.index,
        'commit_count': commit_lang_counts.values
    })

    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Commit language statistics CSV saved: {output_file}")

def save_type_to_csv(output_file, type_counts, total_records):
    result_df = pd.DataFrame({
        'type': type_counts.index,
        'count': type_counts.values,
        'percentage': (type_counts.values / total_records * 100).round(2)
    })

    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Type statistics CSV saved: {output_file}")

def save_scope_to_csv(output_file, scope_counts, total_records):
    result_df = pd.DataFrame({
        'scope': scope_counts.index,
        'count': scope_counts.values,
        'percentage': (scope_counts.values / total_records * 100).round(2)
    })

    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Scope statistics CSV saved: {output_file}")

def main():
    input_file = "./output/ccs_commits.parquet"
    output_dir = "./output/analyze_report"

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"Output directory created: {output_dir}\n")

    analyze_ccs_statistics(
        input_file=input_file,
        output_dir=output_dir,
        top_n=30  # Display and save Top 30
    )

if __name__ == "__main__":
    main()