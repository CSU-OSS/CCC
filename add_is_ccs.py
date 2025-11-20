"""
为CCS Commits数据集添加is_CCS字段

该脚本读取已筛选的CCS仓库commit数据集，逐条检查commit message是否符合CCS规范，
并在原始数据中添加is_CCS字段（1表示符合，0表示不符合）。
"""

import sys
import re
import pandas as pd
from pathlib import Path
from typing import Optional


class CommitCCSChecker:
    
    def __init__(self):
        # Conventional Commits格式的正则表达式
        # 格式: type[(scope)][!]: description
        # 不限制type的具体值，只要求是字母组成即可，不区分大小写
        self.ccs_pattern = re.compile(
            r'^([a-zA-Z]+)'
            r'(\(.+?\))?!?:\s.+',
            re.IGNORECASE
        )
    
    def is_valid_string(self, value) -> bool:
        if value is None:
            return False
        
        if isinstance(value, str):
            return bool(value.strip())
        
        return False
    
    def check_commit(self, message: str) -> bool:
        if not self.is_valid_string(message):
            return False
        
        first_line = message.split('\n')[0].strip()
        is_ccs = bool(self.ccs_pattern.match(first_line))
        
        return is_ccs


def safe_extract_message(value):
    if value is None:
        return None
    
    if hasattr(value, 'iloc'):
        return value.iloc[0] if len(value) > 0 else None
    
    if hasattr(value, 'item'):
        try:
            return value.item()
        except (ValueError, AttributeError):
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


def add_ccs_field_to_dataset(
    input_file: str,
    output_file: Optional[str] = None,
    batch_size: int = 1000
):
    input_path = Path(input_file)
    
    if not input_path.exists():
        print(f"错误：输入文件不存在: {input_file}")
        return
    
    if output_file is None:
        output_file = input_file
    
    print("=" * 80)
    print("CCS Commits数据集 - 添加is_CCS字段")
    print("=" * 80)
    print(f"输入文件: {input_file}")
    print(f"输出文件: {output_file}")
    print("=" * 80)
    
    checker = CommitCCSChecker()
    

    print("\n正在读取parquet文件...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"总记录数: {total_records:,}")
    
    stats = {
        'total': total_records,
        'valid_message': 0,
        'invalid_message': 0,
        'ccs_compliant': 0,
        'ccs_non_compliant': 0
    }
    
    print("\n开始检查commit message...")
    is_ccs_list = []
    
    for idx, row in df.iterrows():
        if (idx + 1) % batch_size == 0 or (idx + 1) == total_records:
            print(f"已处理: {idx + 1:,}/{total_records:,} ({(idx + 1)/total_records*100:.1f}%)")
        
        message = safe_extract_message(row.get('message'))
        
        if not checker.is_valid_string(message):
            is_ccs_list.append(0)
            stats['invalid_message'] += 1
            continue
        
        stats['valid_message'] += 1
        
        is_ccs = checker.check_commit(message)
        is_ccs_list.append(1 if is_ccs else 0)
        
        if is_ccs:
            stats['ccs_compliant'] += 1
        else:
            stats['ccs_non_compliant'] += 1
    
    df['is_CCS'] = is_ccs_list
    
    print(f"\n正在保存结果到: {output_file}")
    df.to_parquet(output_file, index=False)
    
    print("\n" + "=" * 80)
    print("处理完成 - 统计报告")
    print("=" * 80)
    print(f"总记录数: {stats['total']:,}")
    print(f"有效message: {stats['valid_message']:,}")
    print(f"无效message: {stats['invalid_message']:,}")
    print("-" * 80)
    print(f"符合CCS规范 (is_CCS=1): {stats['ccs_compliant']:,} ({stats['ccs_compliant']/stats['total']*100:.2f}%)")
    print(f"不符合CCS规范 (is_CCS=0): {stats['ccs_non_compliant']:,} ({stats['ccs_non_compliant']/stats['total']*100:.2f}%)")
    
    if stats['valid_message'] > 0:
        compliance_rate = stats['ccs_compliant'] / stats['valid_message'] * 100
        print(f"有效message的CCS符合率: {compliance_rate:.2f}%")
    
    print("=" * 80)
    print(f"\n结果已保存到: {output_file}")
    print("新增字段: is_CCS (1=符合CCS规范, 0=不符合CCS规范)")


def main():
    input_file = "./output/commits_by_repo.parquet"
    
    # 可选：如果不想覆盖原文件，可以指定新的输出文件
    # output_file = ""
    output_file = None  # None表示覆盖原文件
    
    add_ccs_field_to_dataset(
        input_file=input_file,
        output_file=output_file,
        batch_size=1000
    )


if __name__ == "__main__":
    main()
