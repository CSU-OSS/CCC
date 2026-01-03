# C2MBench

## Project Setup Guide

This project is now managed using **Git** and **UV**.

### Environment Requirements

- Python >= 3.8
- Git
- UV package manager

### Quick Start

#### 1. Clone the project

```bash
git clone https://github.com/CSU-OSS/CCC.git
cd CCC
```

#### 2. Install Dependencies

Use UV to automatically create a virtual environment and install all dependencies:

```bash
uv sync
```

#### 3. Activate Virtual Environment

```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

#### 4. Run Script

Execute the script using UV (automatically runs within the virtual environment):

```bash
# Method 1: Use uv run (recommended)
uv run python ccs_repo_processor.py

# Method 2: Run directly after activating the environment
.venv\Scripts\activate
python ccs_repo_processor.py
```

## Obtain the Dataset

1. Obtain the raw CommitChronic dataset by running the following commands in the terminal

```bash
pip install huggingface_hub
huggingface-cli download JetBrains-Research/commit-chronicle --repo-type dataset --local-dir ./commit-chronicle-data
```

After running, the raw CommitChronic dataset is stored in `./commit-chronicle-data/data`

2. Create the .env file

```bash
# Linux/Mac
cp .env.example .env

# Windows PowerShell
Copy-Item .env.example .env
```

Then edit the .env file, replacing `your_github_token_here` with your actual token.

3. Filter the raw dataset by checking if repositories comply with CCS specifications. If the dataset hasn't been downloaded to the current directory, modify `input_dir` to your path on line 584.

```bash
python ccs_repo_processor.py
```

After running, `./output` will contain `commits_by_repo.parquet` and `repo_cache_keyword.json`. The former holds the filtered dataset containing all commits from repositories compliant with CCS specifications, while the latter stores whether each repository meets CCS standards.

4. Determine whether each commit in `commits_by_repo.parquet` complies with the CCS specification, and add a new field `is_CCS` to the dataset.

```
python add_is_ccs.py
```

5. Filter out repositories that contain no CCS-compliant commits based on the `is_CCS` field.

```
python filter_repos.py
```

After running, `./output` will contain `commits_true_ccs_repos.parquet` and `repo_ccs_analysis.json`. The former is the filtered dataset, while the latter details CCS-compliant commits for each repository.

6. Export all entries where `is_CCS` = 1 from repositories meeting `ccs_rate > 80%` into a separate file. Add `commit_type` and `commit_scope` fields to each entry by extracting the type and scope from the commit message.

```bash
python filter_extract_ccs.py
```

After running, the filtered `ccs_commits.parquet` will be stored in `./output`, along with `ccs_commits_analysis.json` containing each `repo`'s `ccs_rate`.

7. Determine each repository's “CCS specification introduction date” based on the earliest occurrence of CCS keywords in the repo. Filter all commits in `ccs_commits.parquet` that occurred before this date.

```bash
python filter_keyword_time.py
```

After running, `ccs_commits.parquet` in `./output` will be filtered, and `ccs_adoption_metadata.json` will be generated to record each repo's “CCS specification introduction date”.

8. Partition the data into `train`, `test`, and `valid` datasets. Apply a final filter to retain only repositories appearing in all three datasets.

```bash
python split_ccs_commits.py
```

After running, the split `train`, `test`, and `valid` datasets will be stored in `./output/ccs_commits_dataset`, and the input file `ccs_commits.parquet` will also undergo corresponding filtering.

9. Convert the `parquet` files in `ccs_commits_dataset` to `json` files (if needed)

```bash
python parquet_json.py
```

After running, the converted `json` datasets will be stored in `./output/ccs_commits_dataset_json`

## Data Analysis

```bash
python analyze_ccs_statistics.py
```

This script analyzes the `./output/ccs_commits.parquet` dataset and generates the `./analyze` directory containing the following five files:

- `ccs_statistics_report.txt` - Full text report
- `repo_language_statistics.csv` - Repository language statistics
- `commit_language_statistics.csv` - Commit language statistics
- `commit_type_statistics.csv` - Commit type statistics
- `commit_scope_statistics.csv` - Commit scope statistics

## Project Structure

```
CCC/
├── .git/                          # Git version control
├── .venv/                         # Virtual environment (not committed to Git)
├── .gitignore                     # Git ignore file configuration
├── pyproject.toml                 # Project configuration and dependency declaration
├── uv.lock                        # Locked dependency versions
├── README.md                      # Project description
├── add_is_ccs.py                  # Add is_CCS field
├── analyze_ccs_statistics.py      # Statistical analysis script
├── ccs_repo_processor.py          # CCS repository processor
├── filter_extract_ccs.py          # Filter extraction script
├── filter_keyword_time.py         # Filter by specification adoption date
├── filter_repos.py                # Filter repositories genuinely adhering to CCS specifications
├── parquet_json.py                # Parquet-to-JSON conversion tool
└── split_ccs_commits.py           # Commit splitting script
```

## Output Files

```
output/
├── commits_by_repo.parquet              # Step 3 output: All commits from repositories containing CCS keywords
├── repo_cache_keyword.json              # Step 3 output: Repository CCS compliance check cache
├── commits_true_ccs_repos.parquet       # Step 5 output: Commits from repositories that genuinely comply with CCS after filtering
├── repo_ccs_analysis.json               # Step 5 output: Detailed CCS compliance analysis per repository
├── ccs_commits_analysis.json            # Step 6 output: Detailed information for repositories with CCS compliance > 80%
├── ccs_commits.parquet                  # Step 7 output: Commits compliant with CCS and dated after “CCS adoption date”
├── ccs_adoption_metadata.json           # Step 7 output: Records the “CCS specification introduction date” for each repository
├── ccs_commits_dataset/                 # Step 8 output: Partitioned training/test/validation sets
│   ├── ccs_commits_train.parquet        # Training set (80%)
│   ├── ccs_commits_test.parquet         # Test set (10%)
│   └── ccs_commits_valid.parquet               # Validation set (10%)
├── ccs_commits_dataset_json/            # Step 9 output: Dataset in JSON format
│   ├── ccs_commits_train.json
│   ├── ccs_commits_test.json
│   └── ccs_commits_valid.json
└── analyze_report/                      # Data analysis outputs
    ├── ccs_statistics_report.txt        # Full statistical report
    ├── repo_language_statistics.csv     # Repository language distribution
    ├── commit_language_statistics.csv   # Commit language distribution
    ├── commit_type_statistics.csv       # Commit type statistics
    └── commit_scope_statistics.csv      # Commit scope statistics
```
