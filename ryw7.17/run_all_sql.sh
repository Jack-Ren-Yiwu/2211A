#!/bin/bash

SQL_DIR="/opt/soft/ddl_tms"  # �ĳ��� .sql �ļ�����Ŀ¼
for sql_file in "$SQL_DIR"/*.sql; do
  echo "����ִ�� $sql_file ..."
  hive -f "$sql_file"
  if [ $? -eq 0 ]; then
    echo "$sql_file ִ�гɹ���"
  else
    echo "$sql_file ִ��ʧ�ܣ����飡"
  fi
done

