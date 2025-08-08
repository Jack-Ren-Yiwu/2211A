package org.example;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

/**
 * @Time : 2022/11/13 17:38
 * @Author :tzh
 * @File : CustomerDeserialization
 * @Project : gmall_flink
 **/
public class CustomerDeserialization implements DebeziumDeserializationSchema<String> {
    /*
          步骤：
              1.获取库名&表名
              2.获取"before"数据
              3.获取"after"数据
              4.获取操作类型 READ DELETE UPDATE CREATE
              5.将字段写入JSON对象
              6.输出数据
              SourceRecord : 接收到数据封装的类
              Collector ： 收集器  （往下游传递数据）

     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1.创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();

        //2.获取库名&表名
        String topic = sourceRecord.topic();//topic='mysql_binlog_source.gmall-2021-flink.base_trademark',
        String[] fields = topic.split("\\.");
        String database = fields[1];//库名
        String tableName = fields[2];//表名

        Struct value = (Struct)sourceRecord.value();
        //3.获取"before"数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null){
            Schema beforeSchema = before.schema();//获取元数据，元数据就是描述数据的数据
            List<Field> beforeFields = beforeSchema.fields();//获取所有的字段
            //循环遍历
            for (int i = 0; i < beforeFields.size(); i++) {
                //获取每一个字段
                Field field = beforeFields.get(i);
                //获取字段对应的value值
                Object beforeValue = before.get(field);

                //将数据转成json
                beforeJson.put(field.name(),beforeValue);
            }
        }

        //4.获取"after"数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null){
            Schema afterSchema = after.schema();
            List<Field> afterFields = afterSchema.fields();
            for (int i = 0; i < afterFields.size(); i++) {
                Field field = afterFields.get(i);
                Object afterValue = after.get(field);
                afterJson.put(field.name(),afterValue);
            }
        }

        //5.获取操作类型 READ DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            //Canal 、Maxwell做统一，因为他们里面用的是insert 而且都是小写
            type = "insert";
        }

        //6.将字段写入JSON对象
        result.put("database",database);
        result.put("tableName",tableName);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        result.put("type",type);

        //7.发送数据至下游
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
