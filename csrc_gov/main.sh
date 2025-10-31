#!/bin/bash

# 项目启动路径
processPath="/data/spider_project/csrc_gov/"
# 项目程序名称
processName="main"

# 切换项目目录
cd ${processPath}

# 查询项目程序是否启动
processList=$(ps -aux | grep ${processPath}${processName}.py | grep -v "grep" | grep -v "vim" | grep -v "vi")
echo $processList

# 项目程序未启动则启动，已启动则提示
if [ -z "$processList" ]
then
        echo "程序开始运行"
        # nohup python3 -u ${processPath}${processName}.py >> ${processPath}log/${processName}_nohup_log.log 2>&1 &
        nohup python3 -u ${processPath}${processName}.py > /dev/null 2>&1 &
else
        echo "已有程序运行"
fi
