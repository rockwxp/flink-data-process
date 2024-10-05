package com.rockwxp.dataprocess.app.func;

import com.alibaba.fastjson.JSONObject;
import com.rockwxp.dataprocess.bean.TableProcess;
import com.rockwxp.dataprocess.common.AppCommon;
import com.rockwxp.dataprocess.util.HbaseUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author rock
 * @date 2024/10/2 11:04
 */
public class DimSinkFunc extends RichSinkFunction<Tuple3<String, JSONObject, TableProcess>> {

    private Connection hbaseConnection = null;

    @Override
    public void open(Configuration parameters) throws Exception {

        //HBase connect
         hbaseConnection = HbaseUtil.getHbaseConnection();
    }

    @Override
    public void invoke(Tuple3<String, JSONObject, TableProcess> value, Context context) throws Exception {
        //get value
        String type = value.f0;
        JSONObject data = value.f1;
        TableProcess tableProcess = value.f2;

        //write hbase
        String sinkTable = tableProcess.getSinkTable();
        String sinkFamily = tableProcess.getSinkFamily();
        String sinkRowKey = tableProcess.getSinkRowKey();
        String rowKeyValue = data.getString(sinkRowKey);

        String[] columns = tableProcess.getSinkColumns().split(",");
        String[] columnValues = new String[columns.length];
        for (int i = 0; i < columns.length; i++){
            columnValues[i] = data.getString(columns[i]);
        }

        if("delete".equals(type)){
            HbaseUtil.deleteRow(hbaseConnection, AppCommon.HBASE_NAMESPACE,sinkTable,rowKeyValue);
        }else {
            HbaseUtil.putRow(hbaseConnection, AppCommon.HBASE_NAMESPACE,sinkTable,sinkFamily,rowKeyValue,columns,columnValues);
        }




    }

    @Override
    public void close() throws Exception {
        hbaseConnection.close();
    }
}
