"""
划分CCS Commits数据集为train/valid/test，并过滤只保留在所有三个数据集中都出现的repo

该脚本执行以下操作：
1. 按时间顺序划分数据集为 train(80%) / valid(10%) / test(10%)
2. 找出在train、valid、test中都出现过的repo
3. 只保留这些repo的commits
4. 保存过滤后的三个数据集
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
    print("数据集划分与过滤统计")
    print("=" * 80)
    
    print("\n【原始数据】")
    print(f"总commits数: {len(original_df):,}")
    print(f"总repos数: {original_df['repo'].nunique():,}")
    
    print("\n【初始划分（按时间）】")
    print(f"Train: {len(train_df):,} commits, {train_df['repo'].nunique():,} repos ({len(train_df)/len(original_df)*100:.1f}%)")
    print(f"Valid: {len(valid_df):,} commits, {valid_df['repo'].nunique():,} repos ({len(valid_df)/len(original_df)*100:.1f}%)")
    print(f"Test:  {len(test_df):,} commits, {test_df['repo'].nunique():,} repos ({len(test_df)/len(original_df)*100:.1f}%)")
    
    print("\n【Repo分布分析】")
    train_repos = set(train_df['repo'].unique())
    valid_repos = set(valid_df['repo'].unique())
    test_repos = set(test_df['repo'].unique())
    
    print(f"只在Train中出现的repos: {len(train_repos - valid_repos - test_repos):,}")
    print(f"只在Valid中出现的repos: {len(valid_repos - train_repos - test_repos):,}")
    print(f"只在Test中出现的repos: {len(test_repos - train_repos - valid_repos):,}")
    print(f"在所有三个数据集中都出现的repos: {len(common_repos):,}")
    
    print("\n【过滤后（只保留共同repos）】")
    total_filtered = len(filtered_train) + len(filtered_valid) + len(filtered_test)
    print(f"Train: {len(filtered_train):,} commits ({len(filtered_train)/total_filtered*100:.1f}%)")
    print(f"Valid: {len(filtered_valid):,} commits ({len(filtered_valid)/total_filtered*100:.1f}%)")
    print(f"Test:  {len(filtered_test):,} commits ({len(filtered_test)/total_filtered*100:.1f}%)")
    print(f"共同repos数: {len(common_repos):,}")
    print(f"总commits数: {total_filtered:,}")
    print(f"数据保留率: {total_filtered/len(original_df)*100:.2f}%")
    
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
    print("CCS Commits数据集划分与过滤")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出目录: {output_dir}")
    print(f"划分比例: Train={train_ratio*100:.0f}%, Valid={valid_ratio*100:.0f}%, Test={100-train_ratio*100-valid_ratio*100:.0f}%")
    print("=" * 80)
    
    print("\n正在读取数据...")
    df = pd.read_parquet(input_file)
    print(f"读取完成，共 {len(df):,} 条记录")
    
    if 'date' not in df.columns:
        print("错误：数据集中缺少date字段！")
        return
    
    if 'repo' not in df.columns:
        print("错误：数据集中缺少repo字段！")
        return
    
    print("\n正在按时间排序...")
    df['date'] = pd.to_datetime(df['date'], format=date_format)
    df = df.sort_values("date").reset_index(drop=True)
    
    print("\n正在按时间划分数据集...")
    train_df, valid_df, test_df = split_dataset_by_time(df, train_ratio, valid_ratio)
    
    print("\n正在查找在所有数据集中都出现的repos...")
    common_repos = get_common_repos(train_df, valid_df, test_df)
    print(f"找到 {len(common_repos):,} 个共同repos")
    
    print("\n正在过滤数据集...")
    filtered_train, filtered_valid, filtered_test = filter_by_common_repos(
        train_df, valid_df, test_df, common_repos
    )
    
    print_statistics(
        df, train_df, valid_df, test_df,
        filtered_train, filtered_valid, filtered_test,
        common_repos
    )
    
    print("\n正在保存数据集...")
    filtered_train.to_parquet(train_path, index=False, engine="pyarrow")
    print(f"✓ Train: {train_path}")
    
    filtered_valid.to_parquet(valid_path, index=False, engine="pyarrow")
    print(f"✓ Valid: {valid_path}")
    
    filtered_test.to_parquet(test_path, index=False, engine="pyarrow")
    print(f"✓ Test:  {test_path}")
    
    if save_filtered_full:
        filtered_full_df = df[df['repo'].isin(common_repos)].copy()
        filtered_full_path = input_file
        print(f"\n正在保存过滤后的完整数据集（覆盖原文件）...")
        filtered_full_df.to_parquet(filtered_full_path, index=False, engine="pyarrow")
        print(f"✓ 完整数据集（已过滤）: {filtered_full_path}")
        print(f"  原始记录数: {len(df):,}")
        print(f"  过滤后记录数: {len(filtered_full_df):,}")
        print(f"  移除记录数: {len(df) - len(filtered_full_df):,}")
    
    print("\n" + "=" * 80)
    print("处理完成！")
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
