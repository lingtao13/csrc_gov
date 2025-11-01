# 证监局公告爬虫框架 (csrc_gov)

本项目是一个为了公告数据而设计的、可扩展的爬虫框架。

它采用分层架构设计，将通用的爬虫工具、标准工作流与具体的业务逻辑解耦，使其易于维护，并能快速扩展以支持新的爬虫项目。

## 核心架构

本项目采用三层抽象（L1-L2-L3）设计：

* **L1 - 框架核心 (`base_spider.py`)**: 提供了所有爬虫的通用能力。
    * 封装了 `MysqlTool`、`OBSTool`、`ProxyTool`、`SnowTool` 等工具。
    * 封装了通用的网络I/O（`_make_request`）、文件下载（`download_file_generic`）和文件上传（`upload_to_obs`）。
    * 处理通用的日志、代理、缓存和错误重试。

* **L2 - 抽象工作流 (`abstract_..._spider.py`)**: 继承自L1，定义了标准的爬虫工作流。
    * `AbstractListSpider`: 定义“获取总页数 -> 循环翻页 -> 获取 -> 解析 -> 存库”的工作流。
    * `AbstractDetailSpider`: 定义“从DB获取任务 -> 循环 -> 获取详情 -> 解析 -> 转PDF -> 上传 -> 存新附件任务”的工作流。
    * `AbstractAttachmentSpider`: 定义“从DB获取附件任务 -> 循环 -> 下载 -> 上传 -> 更新DB”的工作流。

* **L3 - 业务实现 (`projects/csrc_gov/`)**: 继承自L2，只包含 `csrc_gov` 这一个项目的特定业务逻辑。
    * `csrc_gov_list_spider.py`: 实现了如何获取证监局的总页数、如何解析列表页JSON。
    * `csrc_gov_detail_spider.py`: 实现了如何解析证监局的详情页HTML、如何提取附件链接。
    * `csrc_gov_attachment_spider.py`: 实现了如何将下载的附件更新回数据库的特定`flag`和`md5`字段。

## 配置管理

本框架采用分层配置，彻底分离了密钥、环境和业务逻辑。

* `conf/config.yml`: **环境开关**。用于指定当前运行环境是 `pro`（生产）还是 `dev`（开发）。
* `conf/infrastructure.yml`: **基础设施与密钥库**。定义所有环境（`pro`, `dev`）的数据库、OBS等连接凭证。
    * **[!] 警告**: 此文件包含敏感密钥，**必须**被添加到 `.gitignore`，严禁提交到代码仓库。
* `conf/projects/csrc_gov.yml`: **项目业务配置**。包含 `csrc_gov` 项目相关的URL、表名、日志路径、辖区列表等业务参数。

## 依赖安装

本项目依赖以下Python库。请使用 `pip` 安装：

~~~
# 数据库
pymysql
sshtunnel

# 网络请求
requests
retrying

# 解析与处理
lxml
pyyaml
pdfkit  # 注意: 依赖系统安装 wkhtmltopdf
obs-python-sdk # 华为OBS SDK
~~~

*（建议创建一个 `requirements.txt` 文件来管理这些依赖。）*

## 如何运行

本项目使用 `main.py` 作为统一的调度入口。通过命令行参数指定要运行的**项目名称**和**执行阶段**。

### 1. 手动分阶段执行

你可以按顺序手动执行爬虫的三个阶段：

~~~bash
# 1. 运行列表页爬虫
python3 main.py csrc_gov list

# 2. 运行详情页爬虫 (将HTML转为PDF并上传)
python3 main.py csrc_gov detail

# 3. 运行附件爬虫 (下载附件并上传)
python3 main.py csrc_gov attachment
~~~

### 2. 使用脚本自动执行

项目根目录下的 `run_spider.sh` 脚本会按顺序自动执行上述三个阶段，并将所有日志输出到 `log/spider_run_all.log` 文件中。

~~~bash
# 赋予脚本执行权限
chmod +x run_spider.sh

# 启动爬虫 (后台运行)
./run_spider.sh

# 查看实时日志
tail -f log/spider_run_all.log
~~~

## 项目结构

~~~
.
├── conf/
│   ├── config.yml                # 1. 环境总开关 (pro/dev)
│   ├── infrastructure.yml        # 2. 基础设施与密钥 (需 gitignore)
│   └── projects/
│       └── csrc_gov.yml          # 3. csrc_gov 项目业务配置
│
├── tools/                        # 通用工具模块 (mysql, obs, log, ...)
│   ├── mysql_tool.py
│   ├── obs_tool.py
│   └── ...
│
├── base_spider.py                # L1 - 框架核心基类
├── abstract_list_spider.py       # L2 - 列表工作流基类
├── abstract_detail_spider.py     # L2 - 详情工作流基类
├── abstract_attachment_spider.py # L2 - 附件工作流基类
│
├── projects/
│   └── csrc_gov/                 # L3 - csrc_gov 业务实现
│       ├── csrc_gov_list_spider.py
│       ├── csrc_gov_detail_spider.py
│       └── csrc_gov_attachment_spider.py
│
├── temp/                         # PDF 转换用的 HTML/CSS 模板
├── log/                          # 日志目录 (由 .gitignore 排除)
├── cache/                        # 缓存目录 (由 .gitignore 排除)
│
├── main.py                       # 统一调度入口
├── run_spider.sh                 # 自动化执行脚本
└── README.md                     # 本文档
~~~