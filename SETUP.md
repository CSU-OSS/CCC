# 项目设置指南

本项目现已使用 **Git** 和 **UV** 进行管理。

## 环境要求

- Python >= 3.8
- Git
- UV 包管理器

## 快速开始

### 1. 克隆项目（如果从远程仓库）

```bash
git clone <repository-url>
cd CCC
```

### 2. 安装依赖

使用 UV 自动创建虚拟环境并安装所有依赖：

```bash
uv sync
```

### 3. 激活虚拟环境

```bash
# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

### 4. 运行脚本

使用 UV 运行脚本（自动在虚拟环境中执行）：

```bash
# 方式1: 使用 uv run（推荐）
uv run python ccs_repo_processor.py

# 方式2: 激活虚拟环境后直接运行
.venv\Scripts\activate
python ccs_repo_processor.py
```

## 项目结构

```
CCC/
├── .git/                          # Git 版本控制
├── .venv/                         # 虚拟环境（不提交到Git）
├── .gitignore                     # Git 忽略文件配置
├── pyproject.toml                 # 项目配置和依赖声明
├── uv.lock                        # 锁定的依赖版本
├── README.md                      # 项目说明
├── SETUP.md                       # 本文件
├── ccs_repo_processor.py          # CCS仓库处理器
├── add_is_ccs.py                  # 添加is_CCS字段
├── analyze_ccs_statistics.py      # 统计分析脚本
├── filter_extract_ccs.py          # 过滤提取脚本
├── parquet_json.py                # Parquet转JSON工具
└── split_ccs_commits.py           # 分割提交脚本
```

## 依赖管理

### 添加新依赖

```bash
# 添加生产依赖
uv add package-name

# 添加开发依赖
uv add --dev package-name

# 添加特定版本
uv add "package-name>=1.0.0,<2.0.0"
```

### 更新依赖

```bash
# 更新所有依赖
uv lock --upgrade

# 更新特定包
uv lock --upgrade-package package-name
```

### 查看已安装的包

```bash
uv pip list
```

## Git 工作流

### 基本操作

```bash
# 查看状态
git status

# 添加文件到暂存区
git add .

# 提交更改
git commit -m "描述你的更改"

# 查看提交历史
git log --oneline

# 查看差异
git diff
```

### 分支管理

```bash
# 创建新分支
git branch feature-name

# 切换分支
git checkout feature-name

# 创建并切换到新分支
git checkout -b feature-name

# 合并分支
git checkout master
git merge feature-name
```

### 远程仓库（如果需要）

```bash
# 添加远程仓库
git remote add origin <repository-url>

# 推送到远程
git push -u origin master

# 拉取更新
git pull
```

## 团队协作

### 新成员加入项目

1. 克隆仓库
2. 运行 `uv sync` 安装依赖
3. 开始开发

### 更新项目依赖

当其他人更新了依赖（修改了 `pyproject.toml` 或 `uv.lock`）：

```bash
git pull
uv sync
```

## 注意事项

### 不要提交的文件

以下文件/目录已在 `.gitignore` 中配置，不会被提交：

- `.venv/` - 虚拟环境
- `__pycache__/` - Python 缓存
- `output/` - 输出数据
- `commit-chronicle-data/` - 大型数据集
- `*.parquet` - Parquet 数据文件
- `*.json` - JSON 数据文件（除配置文件外）

### 敏感信息

⚠️ **重要**: `ccs_repo_processor.py` 中包含 GitHub Token，请：

1. 不要将真实 Token 提交到公共仓库
2. 使用环境变量或配置文件管理敏感信息
3. 考虑使用 `.env` 文件（并添加到 `.gitignore`）

建议修改：

```python
# 不推荐
self.github_token = "ghp_xxx..."

# 推荐
import os
self.github_token = os.getenv('GITHUB_TOKEN', '')
```

## 常见问题

### Q: UV 命令找不到？

A: 确保 UV 已安装并添加到 PATH：

```bash
# Windows PowerShell
$env:Path = "C:\Users\你的用户名\.local\bin;$env:Path"

# 或重启终端
```

### Q: 依赖安装失败？

A: 尝试清理缓存后重新安装：

```bash
uv cache clean
uv sync
```

### Q: Git 提示 LF/CRLF 警告？

A: 这是正常的行尾符转换警告，可以配置：

```bash
# 配置 Git 自动处理行尾符
git config --global core.autocrlf true  # Windows
git config --global core.autocrlf input # Linux/Mac
```

## 更多资源

- [UV 官方文档](https://docs.astral.sh/uv/)
- [Git 官方文档](https://git-scm.com/doc)
- [Conventional Commits 规范](https://www.conventionalcommits.org/)
