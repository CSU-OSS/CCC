"""
Add is_CCS field to the CCS Commits dataset

This script reads a filtered dataset of commits from CCS repositories,
checks each commit message for compliance with the Conventional Commits (CCS) specification,
and adds an 'is_CCS' field to the original data (1 for compliant, 0 for non-compliant).
"""

import sys
import re
import pandas as pd
from pathlib import Path
from typing import Optional


class CommitCCSChecker:

    def __init__(self):
        # Regular expression for Conventional Commits format
        # Format: type[(scope)][!]: description
        # Allows any alphabetical characters for 'type', case-insensitive
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

        # We only check the first line (subject line) of the commit message
        first_line = message.split('\n')[0].strip()
        is_ccs = bool(self.ccs_pattern.match(first_line))

        return is_ccs


def safe_extract_message(value):
    """Safely extracts message content from various pandas/numpy data types."""
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
        print(f"Error: Input file does not exist: {input_file}")
        return

    if output_file is None:
        output_file = input_file

    print("=" * 80)
    print("CCS Commits Dataset - Adding is_CCS field")
    print("=" * 80)
    print(f"Input file:  {input_file}")
    print(f"Output file: {output_file}")
    print("=" * 80)

    checker = CommitCCSChecker()

    print("\nReading parquet file...")
    df = pd.read_parquet(input_file)
    total_records = len(df)
    print(f"Total records found: {total_records:,}")

    stats = {
        'total': total_records,
        'valid_message': 0,
        'invalid_message': 0,
        'ccs_compliant': 0,
        'ccs_non_compliant': 0
    }

    print("\nStarting commit message validation...")
    is_ccs_list = []

    for idx, row in df.iterrows():
        # Progress logging
        if (idx + 1) % batch_size == 0 or (idx + 1) == total_records:
            print(f"Processed: {idx + 1:,}/{total_records:,} ({(idx + 1) / total_records * 100:.1f}%)")

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

    # Add the new feature column
    df['is_CCS'] = is_ccs_list

    print(f"\nSaving results to: {output_file}")
    df.to_parquet(output_file, index=False)

    print("\n" + "=" * 80)
    print("Processing Complete - Statistical Report")
    print("=" * 80)
    print(f"Total records:           {stats['total']:,}")
    print(f"Valid messages:         {stats['valid_message']:,}")
    print(f"Invalid messages:       {stats['invalid_message']:,}")
    print("-" * 80)
    print(
        f"CCS Compliant (is_CCS=1):     {stats['ccs_compliant']:,} ({stats['ccs_compliant'] / stats['total'] * 100:.2f}%)")
    print(
        f"Non-CCS Compliant (is_CCS=0): {stats['ccs_non_compliant']:,} ({stats['ccs_non_compliant'] / stats['total'] * 100:.2f}%)")

    if stats['valid_message'] > 0:
        compliance_rate = stats['ccs_compliant'] / stats['valid_message'] * 100
        print(f"CCS compliance rate (among valid messages): {compliance_rate:.2f}%")

    print("=" * 80)
    print(f"\nSuccess: Result saved to {output_file}")
    print("New field added: is_CCS (1=Compliant, 0=Non-compliant)")

def main():
    input_file = "./output/commits_by_repo.parquet"

    # Optional: Set a specific output file if you don't want to overwrite the original
    # output_file = "./output/commits_with_ccs_tags.parquet"
    output_file = None  # None means overwrite the input file

    add_ccs_field_to_dataset(
        input_file=input_file,
        output_file=output_file,
        batch_size=1000
    )

if __name__ == "__main__":
    main()