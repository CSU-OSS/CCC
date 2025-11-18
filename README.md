# CCC
## 获取数据集

1. 获取原始CommitChronic数据集，在终端中运行以下指令

```bash
pip install huggingface_hub
huggingface-cli download JetBrains-Research/commit-chronicle --repo-type dataset --local-dir ./commit-chronicle-data
```

运行后，`./commit-chronicle-data/data`中存放着原始CommitChronic数据集。

2. 通过判断数据中的repo是否遵守CCS规范，对原始数据集进行筛选，运行脚本前，在36行填入github_token，然后在584行将`input_dir`改为自己的路径

```bash
python ccs_repo_processor.py
```

运行后，`./output`中会存放`ccs_commits_dataset.parquet`与`repo_cache_keyword.json`，前者中是从原始数据集筛选后得到的数据集，存放所有遵守CCS规范的repo下的所有commit数据，后者存放着每个repo是否符合CCS规范。

3. 判断`ccs_commits_dataset.parquet`中的每一条commit是否符合CCS规范，并在数据集中增加一个字段`is_CCS`，运行前，在219行将`input_file`改为自己的路径

```json
python add_is_ccs.py
```

