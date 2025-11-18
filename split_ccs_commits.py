import os
import pandas as pd

INPUT_PATH  = "./output/ccs_commits.parquet"
OUTPUT_DIR  = "./output/ccs_commits_dataset"

train_path = os.path.join(OUTPUT_DIR, "ccs_commits_train.parquet")
valid_path = os.path.join(OUTPUT_DIR, "ccs_commits_valid.parquet")
test_path  = os.path.join(OUTPUT_DIR, "ccs_commits_test.parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = pd.read_parquet(INPUT_PATH)

df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y %H:%M:%S')
df = df.sort_values("date").reset_index(drop=True)

total = len(df)
train_end = int(total * 0.8)
valid_end = int(total * 0.9)

train_df = df.iloc[:train_end]
valid_df = df.iloc[train_end:valid_end]
test_df  = df.iloc[valid_end:]

train_df.to_parquet(train_path, index=False, engine="pyarrow")
valid_df.to_parquet(valid_path, index=False, engine="pyarrow")
test_df.to_parquet(test_path, index=False, engine="pyarrow")

print(f"total commits: {total}")
print(f"train: {len(train_df)} -> {train_path}")
print(f"valid: {len(valid_df)} -> {valid_path}")
print(f"test : {len(test_df)}  -> {test_path}")
