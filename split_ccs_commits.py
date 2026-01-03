"""
Split CCS Commits dataset into train/valid/test and filter to keep only repos present in all three sets.

This script performs the following:
1. Splits the dataset chronologically into train(80%) / valid(10%) / test(10%)
2. Identifies repositories that appear in all three splits (train, valid, and test)
3. Retains only the commits belonging to these common repositories
4. Saves the three filtered datasets
"""

import os
import pandas as pd
from typing import Set


def split_dataset_by_time(
        df: pd.DataFrame,
        train_ratio: float = 0.8,
        valid_ratio: float = 0.1
) -> tuple:
    total = len(df)
    train_end = int(total * train_ratio)
    valid_end = int(total * (train_ratio + valid_ratio))

    train_df = df.iloc[:train_end].copy()
    valid_df = df.iloc[train_end:valid_end].copy()
    test_df = df.iloc[valid_end:].copy()

    return train_df, valid_df, test_df


def get_common_repos(train_df: pd.DataFrame, valid_df: pd.DataFrame, test_df: pd.DataFrame) -> Set[str]:
    train_repos = set(train_df['repo'].unique())
    valid_repos = set(valid_df['repo'].unique())
    test_repos = set(test_df['repo'].unique())

    common_repos = train_repos & valid_repos & test_repos

    return common_repos


def filter_by_common_repos(
        train_df: pd.DataFrame,
        valid_df: pd.DataFrame,
        test_df: pd.DataFrame,
        common_repos: Set[str]
) -> tuple:
    filtered_train = train_df[train_df['repo'].isin(common_repos)].copy()
    filtered_valid = valid_df[valid_df['repo'].isin(common_repos)].copy()
    filtered_test = test_df[test_df['repo'].isin(common_repos)].copy()

    return filtered_train, filtered_valid, filtered_test


def print_statistics(
        original_df: pd.DataFrame,
        train_df: pd.DataFrame,
        valid_df: pd.DataFrame,
        test_df: pd.DataFrame,
        filtered_train: pd.DataFrame,
        filtered_valid: pd.DataFrame,
        filtered_test: pd.DataFrame,
        common_repos: Set[str]
):
    print("\n" + "=" * 80)
    print("Dataset Splitting and Filtering Statistics")
    print("=" * 80)

    print("\n[Original Data]")
    print(f"Total commits: {len(original_df):,}")
    print(f"Total repos: {original_df['repo'].nunique():,}")

    print("\n[Initial Chronological Split]")
    print(
        f"Train: {len(train_df):,} commits, {train_df['repo'].nunique():,} repos ({len(train_df) / len(original_df) * 100:.1f}%)")
    print(
        f"Valid: {len(valid_df):,} commits, {valid_df['repo'].nunique():,} repos ({len(valid_df) / len(original_df) * 100:.1f}%)")
    print(
        f"Test:  {len(test_df):,} commits, {test_df['repo'].nunique():,} repos ({len(test_df) / len(original_df) * 100:.1f}%)")

    print("\n[Repo Distribution Analysis]")
    train_repos = set(train_df['repo'].unique())
    valid_repos = set(valid_df['repo'].unique())
    test_repos = set(test_df['repo'].unique())

    print(f"Repos only in Train: {len(train_repos - valid_repos - test_repos):,}")
    print(f"Repos only in Valid: {len(valid_repos - train_repos - test_repos):,}")
    print(f"Repos only in Test:  {len(test_repos - train_repos - valid_repos):,}")
    print(f"Repos appearing in all three sets: {len(common_repos):,}")

    print("\n[After Filtering (Common Repos Only)]")
    total_filtered = len(filtered_train) + len(filtered_valid) + len(filtered_test)
    print(f"Train: {len(filtered_train):,} commits ({len(filtered_train) / total_filtered * 100:.1f}%)")
    print(f"Valid: {len(filtered_valid):,} commits ({len(filtered_valid) / total_filtered * 100:.1f}%)")
    print(f"Test:  {len(filtered_test):,} commits ({len(filtered_test) / total_filtered * 100:.1f}%)")
    print(f"Common repos count: {len(common_repos):,}")
    print(f"Total filtered commits: {total_filtered:,}")
    print(f"Data retention rate: {total_filtered / len(original_df) * 100:.2f}%")

    print("=" * 80)


def split_and_filter_ccs_commits(
        input_file: str,
        output_dir: str,
        train_ratio: float = 0.8,
        valid_ratio: float = 0.1,
        date_format: str = '%d.%m.%Y %H:%M:%S',
        save_filtered_full: bool = True
):
    os.makedirs(output_dir, exist_ok=True)

    train_path = os.path.join(output_dir, "ccs_commits_train.parquet")
    valid_path = os.path.join(output_dir, "ccs_commits_valid.parquet")
    test_path = os.path.join(output_dir, "ccs_commits_test.parquet")

    print("=" * 80)
    print("CCS Commits Dataset Splitting and Filtering")
    print("=" * 80)
    print(f"Input file: {input_file}")
    print(f"Output directory: {output_dir}")
    print(
        f"Split ratios: Train={train_ratio * 100:.0f}%, Valid={valid_ratio * 100:.0f}%, Test={100 - train_ratio * 100 - valid_ratio * 100:.0f}%")
    print("=" * 80)

    print("\nReading data...")
    df = pd.read_parquet(input_file)
    print(f"Read complete. Found {len(df):,} records")

    if 'date' not in df.columns:
        print("Error: 'date' field missing in dataset!")
        return

    if 'repo' not in df.columns:
        print("Error: 'repo' field missing in dataset!")
        return

    print("\nSorting by date...")
    df['date'] = pd.to_datetime(df['date'], format=date_format)
    df = df.sort_values("date").reset_index(drop=True)

    print("\nSplitting dataset chronologically...")
    train_df, valid_df, test_df = split_dataset_by_time(df, train_ratio, valid_ratio)

    print("\nIdentifying repositories present in all splits...")
    common_repos = get_common_repos(train_df, valid_df, test_df)
    print(f"Found {len(common_repos):,} common repos")

    print("\nFiltering datasets...")
    filtered_train, filtered_valid, filtered_test = filter_by_common_repos(
        train_df, valid_df, test_df, common_repos
    )

    print_statistics(
        df, train_df, valid_df, test_df,
        filtered_train, filtered_valid, filtered_test,
        common_repos
    )

    print("\nSaving datasets...")
    filtered_train.to_parquet(train_path, index=False, engine="pyarrow")
    print(f"✓ Train: {train_path}")

    filtered_valid.to_parquet(valid_path, index=False, engine="pyarrow")
    print(f"✓ Valid: {valid_path}")

    filtered_test.to_parquet(test_path, index=False, engine="pyarrow")
    print(f"✓ Test:  {test_path}")

    if save_filtered_full:
        filtered_full_df = df[df['repo'].isin(common_repos)].copy()
        filtered_full_path = input_file
        print(f"\nSaving filtered full dataset (overwriting original file)...")
        filtered_full_df.to_parquet(filtered_full_path, index=False, engine="pyarrow")
        print(f"✓ Full dataset (filtered): {filtered_full_path}")
        print(f"  Original records: {len(df):,}")
        print(f"  Filtered records: {len(filtered_full_df):,}")
        print(f"  Removed records:  {len(df) - len(filtered_full_df):,}")

    print("\n" + "=" * 80)
    print("Processing complete!")
    print("=" * 80)


def main():
    input_file = "./output/ccs_commits.parquet"
    output_dir = "./output/ccs_commits_dataset"

    split_and_filter_ccs_commits(
        input_file=input_file,
        output_dir=output_dir,
        train_ratio=0.8,
        valid_ratio=0.1,
        date_format='%d.%m.%Y %H:%M:%S',
        save_filtered_full=True
    )


if __name__ == "__main__":
    main()