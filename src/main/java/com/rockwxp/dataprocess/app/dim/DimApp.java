package com.rockwxp.dataprocess.app.dim;
import com.alibaba.fastjson.JSONObject;

import com.rockwxp.dataprocess.bean.TableProcess;
import com.rockwxp.dataprocess.common.AppCommon;
import com.rockwxp.dataprocess.util.CreateEnvUtil;
import com.rockwxp.dataprocess.util.HbaseUtil;
import com.rockwxp.dataprocess.util.KafkaUtil;
import com.rockwxp.dataprocess.util.MysqlUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
/**
 * @author rock
 * @date 2024/10/2 09:58
 */
public class DimApp {
    public static void main(String[] args) throws Exception {
        // TODO 1 创建流环境
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8081, "financial_lease_dim_app");


        // TODO 2 从kafka中读取topic_db原始数据
        String topicName = AppCommon.KAFKA_ODS_TOPIC;
        String appName = "financial_lease_dim_app";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topicName, appName, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), appName);

        // TODO 3 从mysql中读取配置表数据
        DataStreamSource<String> flinkCDCSource = env.fromSource(CreateEnvUtil.getMySqlSource(), WatermarkStrategy.noWatermarks(), appName);

        // TODO 4 在hbase中创建维度表
        SingleOutputStreamOperator<TableProcess> processStream = flinkCDCSource.process(new ProcessFunction<String, TableProcess>() {

            private Connection hBaseConnection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 获取hbase的连接
                hBaseConnection = HbaseUtil.getHbaseConnection();

            }

            @Override
            public void processElement(String jsonStr, Context ctx, Collector<TableProcess> out) throws Exception {
                // 根据配置表数据 创建表格
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);

                String op = jsonObject.getString("op");
                if ("r".equals(op) || "c".equals(op)) {
                    // 新增表格 需要直接创建
                    createTable(tableProcess);
                } else if ("d".equals(op)) {
                    // 删除表格
                    tableProcess = jsonObject.getObject("before", TableProcess.class);
                    deleteTable(tableProcess);
                } else {
                    // 修改表格
                    // 删除表格
                    deleteTable(jsonObject.getObject("before", TableProcess.class));
                    // 创建表格
                    createTable(tableProcess);
                }

                // 将数据继续往下游传递
                tableProcess.setOperateType(op);
                out.collect(tableProcess);
            }

            public void createTable(TableProcess tableProcess) {
                try {
                    HbaseUtil.createTable(hBaseConnection, AppCommon.HBASE_NAMESPACE, tableProcess.getSinkTable(), tableProcess.getSinkFamily().split(","));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            public void deleteTable(TableProcess tableProcess) {
                try {
                    HbaseUtil.deleteTable(hBaseConnection, AppCommon.HBASE_NAMESPACE, tableProcess.getSinkTable());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void close() throws Exception {
                // 关闭hbase的连接
                HbaseUtil.closeHbaseConnection(hBaseConnection);
            }
        });

        // TODO 5 广播配置表双流合并
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("broadcast_state", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = processStream.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<String, TableProcess> connectedStream = kafkaSource.connect(broadcastStream);

        // TODO 6 处理合并后的主流数据 得到维度表数据
        // 返回值类型为<维度数据的类型,数据本身,维度表元数据>
        SingleOutputStreamOperator<Tuple3<String, JSONObject, TableProcess>> dimProcessStream = connectedStream.process(new BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>>() {
            // 属性存储初始化读取的表格
            private HashMap<String, TableProcess> configMap = new HashMap<String, TableProcess>();

            @Override
            public void open(Configuration parameters) throws Exception {
                java.sql.Connection connection = MysqlUtil.getConnection();
                PreparedStatement ps = connection.prepareStatement("select * from financial_lease_config.table_process");
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    TableProcess tableProcess = new TableProcess();
                    tableProcess.setSourceTable(resultSet.getString(1));
                    tableProcess.setSinkTable(resultSet.getString(2));
                    tableProcess.setSinkFamily(resultSet.getString(3));
                    tableProcess.setSinkColumns(resultSet.getString(4));
                    tableProcess.setSinkRowKey(resultSet.getString(5));
                    configMap.put(tableProcess.getSourceTable(), tableProcess);
                }

                ps.close();
                connection.close();
            }

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                // 处理主流数据
                // 读取广播状态的数据 判断当前数据是否为维度表
                // 如果是维度表 保留数据向下游写出
                ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                JSONObject jsonObject = JSONObject.parseObject(value);
                String type = jsonObject.getString("type");
                // maxwell的数据类型有6种  bootstrap-start bootstrap-complete bootstrap-insert  insert update delete
                if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
                    // 当前为空数据  不需要进行操作
                    return;
                }
                // 判断当前表格是否是维度表
                String tableName = jsonObject.getString("table");
                TableProcess tableProcess = broadcastState.get(tableName);

                // 如果从状态中判断不是维度表  添加一次判断 从configMap
                if (tableProcess == null) {
                    //当前不是维度表 判断是否是主流数据来的太早了
                    tableProcess = configMap.get(tableName);
                }

                if (tableProcess == null) {
                    //当前不是维度表
                    return;
                }
                String[] columns = tableProcess.getSinkColumns().split(",");
                JSONObject data = jsonObject.getJSONObject("data");

                if ("delete".equals(type)) {
                    data = jsonObject.getJSONObject("old");
                } else {
                    data.keySet().removeIf(key -> !Arrays.asList(columns).contains(key));
                }

                out.collect(Tuple3.of(type, data, tableProcess));
            }

            @Override
            public void processBroadcastElement(TableProcess tableProcess, Context ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                // 处理广播流数据
                // 将配置表信息写入到广播状态中
                // 读取当前的广播状态
                BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String op = tableProcess.getOperateType();
                if ("d".equals(op)) {
                    // 删除当前的广播状态
                    broadcastState.remove(tableProcess.getSourceTable());

                    // 还需要删除configMap中的数据
                    configMap.remove(tableProcess.getSourceTable());
                } else {
                    // 不是删除  c r u 都需要将数据写入到广播状态中
                    broadcastState.put(tableProcess.getSourceTable(), tableProcess);
                }

            }
        });

        dimProcessStream.print("dim>>>>");


        // TODO 7 写出到hbase


        // TODO 8 执行任务
        env.execute();
    }
}
