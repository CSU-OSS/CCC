"""
过滤并提取CCS Commits数据集

该脚本处理包含is_CCS字段的数据集，执行以下操作：
1. 保留 is_CCS=1 的记录
2. 从message中提取type和scope字段
3. 输出一个所有记录is_CCS都为1的新parquet文件
"""

import re
import pandas as pd
from pathlib import Path
from typing import Tuple, Optional


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


def filter_and_extract_ccs_commits(
    input_file: str,
    output_file: str,
    batch_size: int = 10000
):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        print(f"错误：输入文件不存在: {input_file}")
        return

    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("CCS Commits数据集 - 过滤并提取type和scope")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print("=" * 80)

    print("\n正在读取parquet文件...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"总记录数: {total_records:,}")

    if 'is_CCS' not in df.columns:
        print("错误：输入文件中不存在 'is_CCS' 字段")
        print("可用字段:", df.columns.tolist())
        return

    ccs_count = (df['is_CCS'] == 1).sum()
    non_ccs_count = (df['is_CCS'] == 0).sum()
    print(f"is_CCS=1 的记录: {ccs_count:,} ({ccs_count/total_records*100:.2f}%)")
    print(f"is_CCS=0 的记录: {non_ccs_count:,} ({non_ccs_count/total_records*100:.2f}%)")

    print("\n正在过滤 is_CCS=1 的记录...")
    filtered_df = df[df['is_CCS'] == 1].copy()
    print(f"过滤后记录数: {len(filtered_df):,}")
    
    if len(filtered_df) == 0:
        print("警告：没有符合条件的记录，不生成输出文件")
        return
    
    message_field = "message"
    print(f"\n提取type和scope...")

    type_list = []
    scope_list = []
    failed_messages = []
    
    for idx, message in enumerate(filtered_df[message_field]):
        if (idx + 1) % batch_size == 0 or (idx + 1) == len(filtered_df):
            print(f"已处理: {idx + 1:,}/{len(filtered_df):,} ({(idx + 1)/len(filtered_df)*100:.1f}%)")
        
        commit_type, scope = parse_conventional_commit(message)
        type_list.append(commit_type)
        scope_list.append(scope)
        
        if commit_type is None:
            failed_messages.append({
                'index': idx,
                'message': message
            })
    
    filtered_df['commit_type'] = type_list
    filtered_df['commit_scope'] = scope_list
    
    print(f"\n正在保存结果到: {output_file}")
    filtered_df.to_parquet(output_file, index=False)
    
    print("\n" + "=" * 80)
    print("处理完成")
    print("=" * 80)
    print(f"输出文件: {output_file}")
    print(f"记录数: {len(filtered_df):,}")
    print(f"新增字段: commit_type, commit_scope")
    print(f"所有记录的 is_CCS 均为 1")
    print("=" * 80)


def main():
    input_file = "./output/commits_true_ccs_repos.parquet"
    output_file = "./output/ccs_commits.parquet"
    
    filter_and_extract_ccs_commits(
        input_file=input_file,
        output_file=output_file,
        batch_size=10000
    )


if __name__ == "__main__":
    main()
