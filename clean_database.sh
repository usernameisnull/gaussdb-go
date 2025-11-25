#!/bin/bash

# 配置参数
HOST="your db host"
PORT="8000"
USER="root"
PASSWORD="your_password"
DBNAME="postgres"
TARGET_DB="mabing_test"

# 切换到 omm 用户并检查数据库是否存在
EXISTS=$(su - omm -c "
gsql -h '$HOST' -U '$USER' -p '$PORT' -W '$PASSWORD' -d '$DBNAME' -t -A -c \"SELECT 1 FROM pg_database WHERE datname = '$TARGET_DB';\"
")

if [ "$EXISTS" = "1" ]; then
    echo "数据库 $TARGET_DB 存在，正在清理连接并删除..."
    su - omm -c "
    gsql -h '$HOST' -U '$USER' -p '$PORT' -W '$PASSWORD' -d '$DBNAME' <<EOF
clean connection to all force for database $TARGET_DB;
drop database $TARGET_DB;
\\q
EOF
    "
    echo "数据库 $TARGET_DB 已成功删除。"
else
    echo "数据库 $TARGET_DB 不存在，无需操作。"
fi

su - omm -c "
    gsql -h '$HOST' -U '$USER' -p '$PORT' -W '$PASSWORD' -d '$DBNAME' <<EOF
create database $TARGET_DB;
\\q
EOF
    "