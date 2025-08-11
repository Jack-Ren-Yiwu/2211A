package util;

import app.common.MedicalCommon;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CreateEnvutil {
    public static StreamExecutionEnvironment  getStreamEnv(Integer prot,String appname){
        Configuration conf = new Configuration();
        //设置web监控端口号
        conf.setInteger(RestOptions.PORT,prot);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        //设置检查点和状态后端
        //启动检查点
        env.enableCheckpointing(10*1000L);
        //设置相邻两个检查点最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10*1000L);
        //设置取消job时检查点的清理模式
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        //设置状态后端存储路径
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-ckpt");
        //设置用户名
        System.setProperty("HADOOP_USER_NAME",MedicalCommon.HADOOP_USER_NAME);

        return env;
    }
}
