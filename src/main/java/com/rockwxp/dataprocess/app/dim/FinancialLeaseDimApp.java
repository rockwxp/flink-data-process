package com.rockwxp.dataprocess.app.dim;

import com.alibaba.fastjson.JSONObject;
import com.rockwxp.dataprocess.app.func.DimSinkFunc;
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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.flink.api.java.tuple.Tuple3;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author rock
 * @date 2024/9/29 16:19
 */
public class FinancialLeaseDimApp {
    public static void main(String[] args) throws Exception {
        // create env
        StreamExecutionEnvironment env = CreateEnvUtil.getStreamEnv(8081, "FinancialLeaseDimApp");
        // read kafka source data
        String topicName = AppCommon.KAFKA_ODS_TOPIC;
        String groupId = "financial_lease_dim_app";
        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer(topicName, groupId, OffsetsInitializer.earliest());
        DataStreamSource<String> kafkaSource = env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), groupId);
        // read mysql config data
        DataStreamSource<String> flinkCDCSource = env.fromSource(CreateEnvUtil.getMySqlSource(), WatermarkStrategy.noWatermarks(), groupId);
        // create hbase dim table
        SingleOutputStreamOperator<TableProcess> processStream = flinkCDCSource.process(new ProcessFunction<String, TableProcess>() {

            private Connection hbaseConnection = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //get connection of hbase
                hbaseConnection = HbaseUtil.getHbaseConnection();
            }

            @Override
            public void close() throws Exception {
                // close hbase
                HbaseUtil.closeHbaseConnection(hbaseConnection);
            }

            @Override
            public void processElement(String jsonStr, Context context, Collector<TableProcess> out) throws Exception {
                //create tables
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                TableProcess tableProcess = jsonObject.getObject("after", TableProcess.class);
                String op = jsonObject.getString("op");
                if ("r".equals(op) || "c".equals(op)) {
                    //create new tables
                    createTable(tableProcess);
                } else if ("d".equals(op)) {
                    //delete table
                    deleteTable(jsonObject.getObject("before", TableProcess.class));
                } else {
                    // update table
                    // delete
                    deleteTable(jsonObject.getObject("before", TableProcess.class));
                    //create
                    createTable(tableProcess);
                }
                tableProcess.setOperateType(op);
                out.collect(tableProcess);
            }

            //create hbase table
            public void createTable(TableProcess tableProcess) {
                try {
                    HbaseUtil.createTable(hbaseConnection, AppCommon.HBASE_NAMESPACE, tableProcess.getSinkTable(), tableProcess.getSinkFamily().split(","));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // delete hbase table
            public void deleteTable(TableProcess tableProcess) {
                try {
                    HbaseUtil.deleteTable(hbaseConnection, AppCommon.HBASE_NAMESPACE, tableProcess.getSinkTable());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }


        });
        // broadcast config table
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("broadcast_state", String.class, TableProcess.class);
        BroadcastStream<TableProcess> broadcastStream = processStream.broadcast(mapStateDescriptor);
        //merge table streams
        BroadcastConnectedStream<String, TableProcess> connectedStream = kafkaSource.connect(broadcastStream);

        // process main data stream for dim table
        SingleOutputStreamOperator<Tuple3<String, JSONObject, TableProcess>> dim_process = connectedStream.process(new BroadcastProcessFunction<String, TableProcess, Tuple3<String, JSONObject, TableProcess>>() {

            //save init data from mysql
            private Map<String, TableProcess> configMap = new HashMap<String, TableProcess>();

            @Override
            public void open(Configuration parameters) throws Exception {
                //get config table from mysql
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
                //get broadcast dim table
                ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                JSONObject jsonObject = JSONObject.parseObject(value);
                String type = jsonObject.getString("type");
                if ("bootstrap-start".equals(type) || "bootstrap-complete".equals(type)) {
                    return;
                }
                String tableName = jsonObject.getString("table");
                //
                TableProcess tableProcess = broadcastState.get(tableName);

                //get data from mysql if BroadcastElement does not have config data
                if (tableProcess == null) {

                    tableProcess = configMap.get(tableName);
                }

                if (tableProcess == null) {
                    return;
                }
                String[] columns = tableProcess.getSinkColumns().split(",");
                JSONObject data = jsonObject.getJSONObject("data");
                if ("delete".equals(type)) {
                    data = jsonObject.getJSONObject("old");
                } else {
                    // remove keys which are not existed in sinkColum
                    data.keySet().removeIf(key -> !Arrays.asList(columns).contains(key));
                }

                out.collect(Tuple3.of(type, data, tableProcess));

            }

            @Override
            public void processBroadcastElement(TableProcess tableProcess, Context ctx, Collector<Tuple3<String, JSONObject, TableProcess>> out) throws Exception {
                // put config table into broadcast state
                BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String op = tableProcess.getOperateType();
                if ("d".equals(op)) {
                    broadcastState.remove(tableProcess.getSourceTable());
                    configMap.remove(tableProcess.getSourceTable());
                } else {
                    broadcastState.put(tableProcess.getSourceTable(), tableProcess);
                }
            }
        });
        dim_process.print("dim>>>");


        // write hbase
        dim_process.addSink(new DimSinkFunc());

        // execute task
        env.execute();
    }
}
