# CCC

## 项目设置指南

本项目现已使用 **Git** 和 **UV** 进行管理。

### 环境要求

- Python >= 3.8
- Git
- UV 包管理器

### 快速开始

#### 1. 克隆项目

```bash
git clone https://github.com/CSU-OSS/CCC.git
cd CCC
```

#### 2. 安装依赖

使用 UV 自动创建虚拟环境并安装所有依赖：

```bash
uv sync
```

#### 3. 激活虚拟环境

```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

#### 4. 运行脚本

使用 UV 运行脚本（自动在虚拟环境中执行）：

```bash
# 方式1: 使用 uv run（推荐）
uv run python ccs_repo_processor.py

# 方式2: 激活虚拟环境后直接运行
.venv\Scripts\activate
python ccs_repo_processor.py
```

## 获取数据集

1. 获取原始CommitChronic数据集，在终端中运行以下指令

```bash
pip install huggingface_hub
huggingface-cli download JetBrains-Research/commit-chronicle --repo-type dataset --local-dir ./commit-chronicle-data
```

运行后，`./commit-chronicle-data/data`中存放着原始CommitChronic数据集

2. 创建.env文件

```bash
# Linux/Mac
cp .env.example .env

# Windows PowerShell
Copy-Item .env.example .env
```

然后编辑 .env 文件，将 your_github_token_here 替换为真实 Token

3. 通过判断数据中的repo是否遵守CCS规范，对原始数据集进行筛选，若数据集未下载到当前目录，需要在584行将`input_dir`改为自己的路径

```bash
python ccs_repo_processor.py
```

运行后，`./output`中会存放`commits_by_repo.parquet`与`repo_cache_keyword.json`，前者中是从原始数据集筛选后得到的数据集，存放所有遵守CCS规范的repo下的所有commit数据，后者存放着每个repo是否符合CCS规范

4. 判断`commits_by_repo.parquet`中的每一条commit是否符合CCS规范，并在数据集中增加一个字段`is_CCS`

```json
python add_is_ccs.py
```

5. 根据字段`is_CCS`，过滤掉那些一个遵守CCS规范的commit都没有的`repo`

```
python filter_repos.py
```

运行后，`./output`中会存放`commits_true_ccs_repos.parquet`与`repo_ccs_analysis.json`，前者是过滤后的数据集，后者是每个仓库的遵守CCS规范的commit具体情况

6. 将数据集中所有`is_CCS` = 1的数据单独导出为一个文件，并为每一条数据增加`commit_type`与`commit_scope`字段，提取message中的type与scope

```bash
python filter_extract_ccs.py
```

运行后，`./output`中会存放新的过滤后的`ccs_commits.parquet`

7. 划分`train`、`test`、`valid`数据集

```bash
python split_ccs_commits.py
```

运行后，`./output/ccs_commits_dataset`中会存放分割的`train`、`test`、`valid`三个数据集

8. 将`ccs_commits_dataset`的`parquet`文件转为`json`文件（如果需要）

```bash
python parquet_json.py
```

运行后，`./output/ccs_commits_dataset_json`中会存放转为`json`格式的数据集

## 数据分析

```bash
python analyze_ccs_statistics.py
```

该脚本分析`./output/ccs_commits.parquet`数据集，并生成`./analyze`目录，存放以下五个文件

- `ccs_statistics_report.txt` - 完整文本报告
- `repo_language_statistics.csv` - 仓库语言统计
- `commit_language_statistics.csv` - 提交语言统计
- `commit_type_statistics.csv` - Type统计
- `commit_scope_statistics.csv` - Scope统计

## 项目结构

```
CCC/
├── .git/                          # Git 版本控制
├── .venv/                         # 虚拟环境（不提交到Git）
├── .gitignore                     # Git 忽略文件配置
├── pyproject.toml                 # 项目配置和依赖声明
├── uv.lock                        # 锁定的依赖版本
├── README.md                      # 项目说明
├── add_is_ccs.py                  # 添加is_CCS字段
├── analyze_ccs_statistics.py      # 统计分析脚本
├── ccs_repo_processor.py          # CCS仓库处理器
├── filter_extract_ccs.py          # 过滤提取脚本
├── filter_repos.py                # 过滤真正遵守CCS规范的仓库
├── parquet_json.py                # Parquet转JSON工具
└── split_ccs_commits.py           # 分割提交脚本
```

## 输出文件

```
output/
├── commits_by_repo.parquet              # 步骤3输出：包含CCS关键词的仓库的所有commit
├── repo_cache_keyword.json              # 步骤3输出：仓库CCS规范检查缓存
├── commits_true_ccs_repos.parquet       # 步骤5输出：过滤后真正遵守CCS规范的仓库commit
├── repo_ccs_analysis.json               # 步骤5输出：每个仓库的CCS符合率详细分析
├── ccs_commits.parquet                  # 步骤6输出：仅包含符合CCS规范的commit（is_CCS=1）
├── ccs_commits_dataset/                 # 步骤7输出：划分的训练/测试/验证集
│   ├── ccs_commits_train.parquet                    # 训练集（80%）
│   ├── ccs_commits_test.parquet                     # 测试集（10%）
│   └── ccs_commits_valid.parquet                    # 验证集（10%）
├── ccs_commits_dataset_json/            # 步骤8输出：JSON格式的数据集
│   ├── ccs_commits_train.json
│   ├── ccs_commits_test.json
│   └── ccs_commits_valid.json
└── analyze_report/                      # 数据分析输出
    ├── ccs_statistics_report.txt        # 完整统计报告
    ├── repo_language_statistics.csv     # 仓库语言分布
    ├── commit_language_statistics.csv   # commit语言分布
    ├── commit_type_statistics.csv       # commit类型统计
    └── commit_scope_statistics.csv      # commit scope统计
```

