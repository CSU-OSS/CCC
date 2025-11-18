"""
CCS Commits数据集统计分析脚本

该脚本读取包含language、repo、message字段的parquet文件，
生成详细的统计报告，包括：
1. 不同仓库的编程语言种类与数量
2. commit的编程语言种类与数量
3. commit的type的种类与数量
4. commit的scope的种类与数量
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
        print(f"错误：输入文件不存在: {input_file}")
        return
    
    if output_dir is None:
        output_dir = input_path.parent
    else:
        output_dir = Path(output_dir)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("CCS Commits数据集 - 统计分析")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出目录: {output_dir}")
    print("=" * 80)
    
    print("\n正在读取parquet文件...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"总记录数: {total_records:,}")
    
    required_fields = ['language', 'repo', 'commit_type', 'commit_scope']
    missing_fields = [f for f in required_fields if f not in df.columns]
    
    if missing_fields:
        print(f"错误：缺少必要字段: {missing_fields}")
        print(f"可用字段: {df.columns.tolist()}")
        return
    
    # 分析1: 不同仓库的编程语言种类与数量
    print("\n" + "=" * 80)
    print("分析1: 不同仓库的编程语言种类与数量")
    print("=" * 80)
    
    repo_language_df = df[['repo', 'language']].drop_duplicates()
    repo_lang_counts = repo_language_df['language'].value_counts()
    
    total_repos = df['repo'].nunique()
    unique_languages_repos = len(repo_lang_counts)
    
    print(f"符合CCS规范的仓库总数: {total_repos:,}")
    print(f"不同语言总数: {unique_languages_repos}")
    
    print(f"\n语言分布（仓库数量）:")
    print(f"{'语言':<20} {'仓库数量':<15}")
    print("-" * 40)
    for lang, count in repo_lang_counts.items():
        lang_display = str(lang) if pd.notna(lang) else 'None'
        print(f"{lang_display:<20} {count:<15}")
    
    # 分析2: commit的编程语言种类与数量
    print("\n" + "=" * 80)
    print("分析2: commit的编程语言种类与数量")
    print("=" * 80)
    
    commit_lang_counts = df['language'].value_counts()
    unique_languages_commits = len(commit_lang_counts)
    
    print(f"符合CCS规范的提交总数: {total_records:,}")
    print(f"不同语言总数: {unique_languages_commits}")
    
    print(f"\n语言分布（提交数量）:")
    print(f"{'语言':<20} {'提交数量':<15}")
    print("-" * 40)
    for lang, count in commit_lang_counts.items():
        lang_display = str(lang) if pd.notna(lang) else 'None'
        print(f"{lang_display:<20} {count:<15}")
    
    # 分析3: commit的type种类与数量
    print("\n" + "=" * 80)
    print("分析3: commit的type种类与数量")
    print("=" * 80)
    
    type_series = df['commit_type'].fillna('None')
    type_counts = type_series.value_counts()
    
    valid_type_count = (df['commit_type'].notna()).sum()
    none_type_count = (df['commit_type'].isna()).sum()
    
    print(f"有效type记录数: {valid_type_count:,} ({valid_type_count/total_records*100:.2f}%)")
    print(f"无效type记录数: {none_type_count:,} ({none_type_count/total_records*100:.2f}%)")
    print(f"type种类数: {len(type_counts)}")
    
    print(f"\nType分布（Top {min(top_n, len(type_counts))}）:")
    print(f"{'Type':<20} {'提交数量':<15}")
    print("-" * 40)
    for commit_type, count in type_counts.head(top_n).items():
        print(f"{commit_type:<20} {count:<15}")
    
    # 分析4: commit的scope种类与数量
    print("\n" + "=" * 80)
    print("分析4: commit的scope种类与数量")
    print("=" * 80)
    
    scope_series = df['commit_scope'].fillna('None')
    scope_counts = scope_series.value_counts()
    
    valid_scope_count = (df['commit_scope'].notna()).sum()
    none_scope_count = (df['commit_scope'].isna()).sum()
    
    print(f"有scope记录数: {valid_scope_count:,} ({valid_scope_count/total_records*100:.2f}%)")
    print(f"无scope记录数: {none_scope_count:,} ({none_scope_count/total_records*100:.2f}%)")
    print(f"scope种类数: {len(scope_counts)}")
    
    print(f"\nScope分布（Top {min(top_n, len(scope_counts))}）:")
    print(f"{'Scope':<40} {'提交数量':<15}")
    print("-" * 60)
    for scope, count in scope_counts.head(top_n).items():
        scope_display = str(scope)[:40]
        print(f"{scope_display:<40} {count:<15}")
    
    # 保存统计结果到文本文件
    print("\n正在保存统计结果...")
    txt_output = output_dir / "ccs_statistics_report.txt"
    save_text_report(txt_output, df, repo_lang_counts, commit_lang_counts, type_counts, scope_counts, top_n)
    
    # 保存各项统计到CSV
    repo_lang_csv = output_dir / "repo_language_statistics.csv"
    save_repo_language_to_csv(repo_lang_csv, repo_lang_counts)
    
    commit_lang_csv = output_dir / "commit_language_statistics.csv"
    save_commit_language_to_csv(commit_lang_csv, commit_lang_counts)
    
    type_csv = output_dir / "commit_type_statistics.csv"
    save_type_to_csv(type_csv, type_counts, total_records)
    
    scope_csv = output_dir / "commit_scope_statistics.csv"
    save_scope_to_csv(scope_csv, scope_counts, total_records)
    
    print("\n" + "=" * 80)
    print("统计分析完成")
    print("=" * 80)
    print(f"文本报告: {txt_output}")
    print(f"仓库语言统计CSV: {repo_lang_csv}")
    print(f"提交语言统计CSV: {commit_lang_csv}")
    print(f"Type统计CSV: {type_csv}")
    print(f"Scope统计CSV: {scope_csv}")
    print("=" * 80)


def save_text_report(output_file, df, repo_lang_counts, commit_lang_counts, type_counts, scope_counts, top_n):
    total_records = len(df)
    total_repos = df['repo'].nunique()
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("CCS Commits数据集统计报告\n")
        f.write("=" * 80 + "\n\n")
        
        # 分析1: 仓库语言统计
        f.write("分析1: 不同仓库的编程语言种类与数量\n")
        f.write("-" * 80 + "\n")
        f.write(f"符合CCS规范的仓库总数: {total_repos:,}\n")
        f.write(f"不同语言总数: {len(repo_lang_counts)}\n\n")
        
        f.write(f"{'语言':<20} {'仓库数量':<15}\n")
        f.write("-" * 40 + "\n")
        for lang, count in repo_lang_counts.items():
            lang_display = str(lang) if pd.notna(lang) else 'None'
            f.write(f"{lang_display:<20} {count:<15}\n")
        
        # 分析2: 提交语言统计
        f.write("\n\n分析2: commit的编程语言种类与数量\n")
        f.write("-" * 80 + "\n")
        f.write(f"符合CCS规范的提交总数: {total_records:,}\n")
        f.write(f"不同语言总数: {len(commit_lang_counts)}\n\n")
        
        f.write(f"{'语言':<20} {'提交数量':<15}\n")
        f.write("-" * 40 + "\n")
        for lang, count in commit_lang_counts.items():
            lang_display = str(lang) if pd.notna(lang) else 'None'
            f.write(f"{lang_display:<20} {count:<15}\n")
        
        # 分析3: Type统计
        f.write("\n\n分析3: commit的type种类与数量\n")
        f.write("-" * 80 + "\n")
        
        valid_type_count = (df['commit_type'].notna()).sum()
        none_type_count = (df['commit_type'].isna()).sum()
        
        f.write(f"type种类数: {len(type_counts)}\n\n")
        
        f.write(f"{'Type':<20} {'提交数量':<15}\n")
        f.write("-" * 40 + "\n")
        for commit_type, count in type_counts.items():
            f.write(f"{commit_type:<20} {count:<15}\n")
        
        # 分析4: Scope统计
        f.write("\n\n分析4: commit的scope种类与数量\n")
        f.write("-" * 80 + "\n")
        
        valid_scope_count = (df['commit_scope'].notna()).sum()
        none_scope_count = (df['commit_scope'].isna()).sum()
        
        f.write(f"scope种类数: {len(scope_counts)}\n\n")
        
        f.write(f"Scope分布（Top {min(top_n, len(scope_counts))}）:\n")
        f.write(f"{'Scope':<40} {'提交数量':<15}\n")
        f.write("-" * 60 + "\n")
        for scope, count in scope_counts.head(top_n).items():
            f.write(f"{str(scope):<40} {count:<15}\n")
    
    print(f"文本报告已保存: {output_file}")


def save_repo_language_to_csv(output_file, repo_lang_counts):
    result_df = pd.DataFrame({
        'language': repo_lang_counts.index,
        'repo_count': repo_lang_counts.values
    })
    
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"仓库语言统计CSV已保存: {output_file}")


def save_commit_language_to_csv(output_file, commit_lang_counts):
    result_df = pd.DataFrame({
        'language': commit_lang_counts.index,
        'commit_count': commit_lang_counts.values
    })
    
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"提交语言统计CSV已保存: {output_file}")


def save_type_to_csv(output_file, type_counts, total_records):
    result_df = pd.DataFrame({
        'type': type_counts.index,
        'count': type_counts.values,
        'percentage': (type_counts.values / total_records * 100).round(2)
    })
    
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Type统计CSV已保存: {output_file}")


def save_scope_to_csv(output_file, scope_counts, total_records):
    result_df = pd.DataFrame({
        'scope': scope_counts.index,
        'count': scope_counts.values,
        'percentage': (scope_counts.values / total_records * 100).round(2)
    })
    
    result_df.to_csv(output_file, index=False, encoding='utf-8')
    print(f"Scope统计CSV已保存: {output_file}")


def main():
    input_file = "./output/ccs_commits.parquet"
    output_dir = "./analyze_report"
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    print(f"输出目录已创建: {output_dir}\n")
    
    analyze_ccs_statistics(
        input_file=input_file,
        output_dir=output_dir,
        top_n=30  # 显示和保存Top 30
    )


if __name__ == "__main__":
    main()
