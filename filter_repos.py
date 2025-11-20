"""
过滤真正遵守CCS规范的仓库
该脚本读取带有is_CCS字段的数据集，过滤掉所有commit的is_CCS都为0的仓库，
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
        print("\n正在分析各仓库的CCS符合情况...")
        
        repo_groups = df.groupby('repo')
        
        for repo_name, group in repo_groups:
            total_commits = len(group)
            ccs_commits = group['is_CCS'].sum()
            non_ccs_commits = total_commits - ccs_commits
            
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
        print("\n正在过滤数据集...")
        
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
        print("仓库CCS符合情况分析")
        print("=" * 80)

        true_ccs_repos = {k: v for k, v in self.repo_ccs_status.items() if v['is_true_ccs']}
        false_ccs_repos = {k: v for k, v in self.repo_ccs_status.items() if not v['is_true_ccs']}
        
        print(f"\n总仓库数: {self.stats['total_repos']:,}")
        print(f"真正遵守CCS规范的仓库: {self.stats['true_ccs_repos']:,} ({self.stats['true_ccs_repos']/self.stats['total_repos']*100:.2f}%)")
        print(f"未真正遵守CCS规范的仓库: {self.stats['false_ccs_repos']:,} ({self.stats['false_ccs_repos']/self.stats['total_repos']*100:.2f}%)")

        if false_ccs_repos:
            print(f"\n未真正遵守CCS规范的仓库列表 (所有commit的is_CCS都为0):")
            print("-" * 80)
            for i, (repo, status) in enumerate(false_ccs_repos.items(), 1):
                print(f"  [{i}] {repo} (commits: {status['total_commits']})")

        if true_ccs_repos:
            print(f"\nCCS符合率最高的前{min(top_n, len(true_ccs_repos))}个仓库:")
            print("-" * 80)
            sorted_repos = sorted(
                true_ccs_repos.items(),
                key=lambda x: x[1]['ccs_rate'],
                reverse=True
            )[:top_n]
            
            for i, (repo, status) in enumerate(sorted_repos, 1):
                print(f"  [{i}] {repo}")
                print(f"      总commits: {status['total_commits']}, "
                      f"符合CCS: {status['ccs_commits']}, "
                      f"不符合: {status['non_ccs_commits']}, "
                      f"符合率: {status['ccs_rate']*100:.2f}%")
        
        print("=" * 80)
    
    def print_final_stats(self) -> None:
        print("\n" + "=" * 80)
        print("过滤结果统计")
        print("=" * 80)
        print(f"原始记录数: {self.stats['total_records']:,}")
        print(f"过滤后记录数: {self.stats['filtered_records']:,}")
        print(f"移除的记录数: {self.stats['removed_records']:,}")
        print(f"数据保留率: {self.stats['filtered_records']/self.stats['total_records']*100:.2f}%")
        print("-" * 80)
        print(f"原始仓库数: {self.stats['total_repos']:,}")
        print(f"保留的仓库数: {self.stats['true_ccs_repos']:,}")
        print(f"移除的仓库数: {self.stats['false_ccs_repos']:,}")
        print(f"仓库保留率: {self.stats['true_ccs_repos']/self.stats['total_repos']*100:.2f}%")
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
        
        print(f"\n仓库分析结果已保存到: {output_file}")


def filter_true_ccs_repos(
    input_file: str,
    output_file: str,
    analysis_file: str = None
):
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"错误：输入文件不存在: {input_file}")
        return
    
    print("=" * 80)
    print("过滤真正遵守CCS规范的仓库")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    if analysis_file:
        print(f"分析文件: {analysis_file}")
    print("=" * 80)

    print("\n正在读取数据集...")
    df = pd.read_parquet(input_file)
    print(f"读取完成，共 {len(df):,} 条记录")

    if 'is_CCS' not in df.columns:
        print("错误：数据集中缺少is_CCS字段！")
        print("请先运行add_is_ccs.py脚本添加is_CCS字段。")
        return
    
    if 'repo' not in df.columns:
        print("错误：数据集中缺少repo字段！")
        return

    filter_obj = TrueCCSRepoFilter()
    filter_obj.analyze_repos(df)

    filter_obj.print_repo_analysis(top_n=10)

    filtered_df = filter_obj.filter_dataset(df)

    print(f"\n正在保存过滤后的数据到: {output_file}")
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_parquet(output_file, index=False)

    if analysis_file:
        filter_obj.save_repo_analysis(analysis_file)

    filter_obj.print_final_stats()
    
    print(f"\n过滤完成！结果已保存到: {output_file}")


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
