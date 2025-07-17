#!/bin/bash

SQL_DIR="/opt/soft/ddl_tms"  # 改成你 .sql 文件所在目录
for sql_file in "$SQL_DIR"/*.sql; do
  echo "正在执行 $sql_file ..."
  hive -f "$sql_file"
  if [ $? -eq 0 ]; then
    echo "$sql_file 执行成功！"
  else
    echo "$sql_file 执行失败，请检查！"
  fi
done

