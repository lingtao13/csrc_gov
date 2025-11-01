#!/bin/bash

# --- 1. 配置 ---

# 项目启动路径 (请根据你的服务器实际路径修改)
# 假设 main.py 位于 /data/spider_project/ 下
processPath="/data/spider_project/"

# 主程序名称 (我们的新调度器)
mainProgram="main.py"

# 要运行的项目 (main.py 的第一个参数)
PROJECT_NAME="csrc_gov"

# 日志文件
# 将所有阶段的输出都打印到这个文件
LOG_FILE="${processPath}log/spider_run_all.log"

# --- 2. 检查 ---

# 切换项目目录
cd ${processPath}
if [ $? -ne 0 ]; then
    echo "错误: 无法切换到目录 ${processPath}"
    exit 1
fi

# (可选) 激活虚拟环境, 如果你使用了 (如 temp02.sh 所示)
# echo "正在激活虚拟环境..."
# source /path/to/your/venv/bin/activate
# if [ $? -ne 0 ]; then
#     echo "错误: 虚拟环境激活失败"
#     exit 1
# fi

# 查询项目程序是否启动
# 我们检查 main.py 是否正在被执行
processList=$(ps -aux | grep "${mainProgram}" | grep "${PROJECT_NAME}" | grep -v "grep" | grep -v "vim" | grep -v "vi")
echo "当前运行的进程:"
echo $processList

# --- 3. 启动 ---

# 项目程序未启动则启动，已启动则提示
if [ -z "$processList" ]
then
        echo "程序未运行, 开始启动..."

        # 使用 nohup 在后台顺序执行三个阶段
        # 1. 创建/清空日志文件
        echo "启动时间: $(date)" > ${LOG_FILE}

        # 2. 顺序执行, 确保 list -> detail -> attachment
        #    使用 ( ... ) 将所有命令组合在一个子 shell 中
        #    使用 >> ${LOG_FILE} 2>&1 & 将所有输出(标准和错误)附加到日志文件, 并在后台运行
        (
            echo "--- (1/3) 开始执行 [${PROJECT_NAME} list] ---"
            python3 -u ${mainProgram} ${PROJECT_NAME} list

            echo "--- (2/3) 开始执行 [${PROJECT_NAME} detail] ---"
            python3 -u ${mainProgram} ${PROJECT_NAME} detail

            echo "--- (3/3) 开始执行 [${PROJECT_NAME} attachment] ---"
            python3 -u ${mainProgram} ${PROJECT_NAME} attachment

            echo "--- 所有阶段执行完毕 ---"
        ) >> ${LOG_FILE} 2>&1 &

        echo "程序已在后台启动。"
        echo "你可以使用 'tail -f ${LOG_FILE}' 来查看实时日志。"
else
        echo "已有程序在运行, 本次不启动。"
fi