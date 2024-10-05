package com.rockwxp.dataprocess.util;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.rockwxp.dataprocess.common.AppCommon;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @author rock
 * @date 2024/9/29 16:24
 */
public class CreateEnvUtil {

    public static StreamExecutionEnvironment getStreamEnv(Integer port,String appName){
        //create WebUI Client

        //1. create stream env
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 2 set checkpoint and StateBackend
        env.enableCheckpointing(10*1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10*1000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(AppCommon.HDFS_URI_PREFIX + appName);
        System.setProperty("HADOOP_USER_NAME", AppCommon.HADOOP_USER_NAME);

        return env;
    }

    public static MySqlSource<String> getMySqlSource(){
        return  MySqlSource.<String>builder()
                .hostname(AppCommon.MYSQL_HOSTNAME)
                .port(AppCommon.MYSQL_PORT)
                .databaseList(AppCommon.FINANCIAL_CONFIG_DATABASE)
                .tableList(AppCommon.FINANCIAL_CONFIG_TABLE)
                .username(AppCommon.MYSQL_USERNAME)
                .password(AppCommon.MYSQL_PASSWD)
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
    }
}
