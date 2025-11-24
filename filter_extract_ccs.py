"""
过滤并提取高CCS符合率仓库的Commits数据集

该脚本处理包含is_CCS字段的数据集，执行以下操作：
1. 计算每个仓库的CCS符合率 (ccs_rate = ccs_commits / total_commits)
2. 只保留 ccs_rate > 80% 的仓库
3. 从这些仓库中只提取 is_CCS=1 的commits
4. 从message中提取type和scope字段
5. 输出一个新的parquet文件
"""

import re
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional, Dict


def parse_conventional_commit(message: str) -> Tuple[Optional[str], Optional[str]]:
    if not message or not isinstance(message, str):
        return None, None

    first_line = message.split('\n')[0].strip()

    simple_pattern = r'^([a-zA-Z]+)!?:\s*(.+)'
    simple_match = re.match(simple_pattern, first_line)

    type_with_scope_pattern = r'^([a-zA-Z]+)\('
    has_scope = re.match(type_with_scope_pattern, first_line)
    
    if has_scope:
        # 有scope的情况，需要找到匹配的右括号
        type_match = re.match(r'^([a-zA-Z]+)\(', first_line)
        if type_match:
            commit_type = type_match.group(1).lower()
            start_pos = len(commit_type) + 1
            
            # 查找匹配的右括号（支持嵌套括号）
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
                # 检查右括号后是否是 !: 或 :
                remaining = first_line[scope_end + 1:].lstrip()
                if remaining.startswith('!:') or remaining.startswith(':'):
                    return commit_type, scope if scope else None
    
    elif simple_match:
        commit_type = simple_match.group(1).lower()
        return commit_type, None
    
    return None, None


def calculate_repo_ccs_rates(df: pd.DataFrame) -> Dict[str, Dict]:
    print("\n正在计算各仓库的CCS符合率...")
    
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
    print(f"\n正在过滤 ccs_rate > {min_ccs_rate*100:.0f}% 的仓库...")
    
    high_rate_repos = [
        repo for repo, stats in repo_stats.items()
        if stats['ccs_rate'] > min_ccs_rate
    ]
    
    print(f"符合条件的仓库数: {len(high_rate_repos)} / {len(repo_stats)}")
    
    filtered_df = df[(df['repo'].isin(high_rate_repos)) & (df['is_CCS'] == 1)].copy()
    
    return filtered_df


def print_repo_statistics(
    repo_stats: Dict[str, Dict],
    min_ccs_rate: float = 0.8,
    top_n: int = 10
) -> None:
    print("\n" + "=" * 80)
    print("仓库CCS符合率统计")
    print("=" * 80)
    
    total_repos = len(repo_stats)
    high_rate_repos = {k: v for k, v in repo_stats.items() if v['ccs_rate'] > min_ccs_rate}
    low_rate_repos = {k: v for k, v in repo_stats.items() if v['ccs_rate'] <= min_ccs_rate}
    
    print(f"\n总仓库数: {total_repos:,}")
    print(f"CCS符合率 > {min_ccs_rate*100:.0f}% 的仓库: {len(high_rate_repos):,} ({len(high_rate_repos)/total_repos*100:.2f}%)")
    print(f"CCS符合率 ≤ {min_ccs_rate*100:.0f}% 的仓库: {len(low_rate_repos):,} ({len(low_rate_repos)/total_repos*100:.2f}%)")
    
    if high_rate_repos:
        print(f"\nCCS符合率最高的前{min(top_n, len(high_rate_repos))}个仓库:")
        print("-" * 80)
        sorted_repos = sorted(
            high_rate_repos.items(),
            key=lambda x: x[1]['ccs_rate'],
            reverse=True
        )[:top_n]
        
        for i, (repo, stats) in enumerate(sorted_repos, 1):
            print(f"  [{i}] {repo}")
            print(f"      总commits: {stats['total_commits']}, "
                  f"符合CCS: {stats['ccs_commits']}, "
                  f"不符合: {stats['non_ccs_commits']}, "
                  f"符合率: {stats['ccs_rate']*100:.2f}%")
    
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
        print(f"错误：输入文件不存在: {input_file}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print(f"高CCS符合率仓库Commits数据集 - 过滤并提取 (ccs_rate > {min_ccs_rate*100:.0f}%)")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print(f"CCS符合率阈值: > {min_ccs_rate*100:.0f}%")
    print("=" * 80)

    print("\n正在读取parquet文件...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"总记录数: {total_records:,}")

    if 'is_CCS' not in df.columns:
        print("错误：输入文件中不存在 'is_CCS' 字段")
        print("可用字段:", df.columns.tolist())
        return
    
    if 'repo' not in df.columns:
        print("错误：输入文件中不存在 'repo' 字段")
        print("可用字段:", df.columns.tolist())
        return

    repo_stats = calculate_repo_ccs_rates(df)
    
    print_repo_statistics(repo_stats, min_ccs_rate, top_n=10)
    
    filtered_df = filter_high_ccs_rate_repos(df, repo_stats, min_ccs_rate)
    
    print(f"\n过滤后记录数: {len(filtered_df):,}")
    print(f"数据保留率: {len(filtered_df)/total_records*100:.2f}%")
    
    if len(filtered_df) == 0:
        print("警告：没有符合条件的记录，不生成输出文件")
        return
    
    ccs_count = (filtered_df['is_CCS'] == 1).sum()
    print(f"\n过滤后的数据中:")
    print(f"  is_CCS=1 的记录: {ccs_count:,} (100.00%)")
    print(f"  所有记录均为符合CCS规范的commits")
    
    message_field = "message"
    print(f"\n正在提取commit type和scope...")

    type_list = []
    scope_list = []
    
    for idx, message in enumerate(filtered_df[message_field]):
        if (idx + 1) % batch_size == 0 or (idx + 1) == len(filtered_df):
            print(f"已处理: {idx + 1:,}/{len(filtered_df):,} ({(idx + 1)/len(filtered_df)*100:.1f}%)")
        
        commit_type, scope = parse_conventional_commit(message)
        type_list.append(commit_type)
        scope_list.append(scope)
    
    filtered_df['commit_type'] = type_list
    filtered_df['commit_scope'] = scope_list
    
    print(f"\n正在保存结果到: {output_file}")
    filtered_df.to_parquet(output_file, index=False)
    
    if save_analysis:
        analysis_file = output_path.parent / f"{output_path.stem}_analysis.json"
        save_repo_analysis(repo_stats, filtered_df, min_ccs_rate, str(analysis_file))
    
    print("\n" + "=" * 80)
    print("处理完成")
    print("=" * 80)
    print(f"输出文件: {output_file}")
    print(f"记录数: {len(filtered_df):,}")
    print(f"仓库数: {filtered_df['repo'].nunique():,}")
    print(f"新增字段: commit_type, commit_scope")
    print(f"所有记录来自 ccs_rate > {min_ccs_rate*100:.0f}% 的仓库")
    print(f"所有记录的 is_CCS 均为 1")
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
            'description': f'只保留CCS符合率 > {min_ccs_rate*100:.0f}% 的仓库'
        },
        'statistics': {
            'total_repos': len(repo_stats),
            'filtered_repos': len(high_rate_repos),
            'removed_repos': len(repo_stats) - len(high_rate_repos),
            'total_commits': len(filtered_df),
            'ccs_commits': int((filtered_df['is_CCS'] == 1).sum()),
            'note': '只包含is_CCS=1的commits'
        },
        'high_rate_repos': high_rate_repos
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n仓库分析结果已保存到: {output_file}")


def main():
    input_file = "./output/commits_true_ccs_repos.parquet"
    output_file = "./output/ccs_commits.parquet"
    
    filter_and_extract_high_rate_commits(
        input_file=input_file,
        output_file=output_file,
        min_ccs_rate=0.8,  # 80%阈值
        batch_size=10000,
        save_analysis=True
    )

if __name__ == "__main__":
    main()
