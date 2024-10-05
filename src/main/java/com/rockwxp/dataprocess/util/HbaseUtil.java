package com.rockwxp.dataprocess.util;

import com.rockwxp.dataprocess.common.AppCommon;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author rock
 * @date 2024/9/30 23:22
 */
public class HbaseUtil {
    /**
     * create hbase connection
     * @return Hbase Connection
     */
    public static Connection getHbaseConnection() {
        Configuration conf = HBaseConfiguration.create();
        conf.set(AppCommon.HBASE_ZOOKEEPER_QUORUM, AppCommon.HBASE_ZOOKEEPER_QUORUM_HOST);
        conf.set(AppCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT, AppCommon.HBASE_ZOOKEEPER_PROPERTY_CLIENT_PORT_VALUE);
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * close hbase connection
     * @param connection
     */
    public static void closeHbaseConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void createTable(Connection hbaseConnection, String hbaseNamespace, String tableName, String... familyNames) throws IOException {

        if(familyNames == null || familyNames.length == 0) {
            throw new RuntimeException("familyNames is null or empty");
        }
            //ddl
            Admin admin = hbaseConnection.getAdmin();
            TableName tableName1 = TableName.valueOf(hbaseNamespace, tableName);
        try {
            // if the table is existed
            if(admin.tableExists(tableName1)) {
                System.out.println(hbaseNamespace+":"+tableName+" already exists");
            }else {
                TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName1);
                for (String familyName : familyNames) {
                    ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(familyName));
                    tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
                }
                admin.createTable(tableDescriptorBuilder.build());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        admin.close();
    }

    public static void deleteTable(Connection hbaseConnection, String hbaseNamespace, String tableName) throws IOException {
        Admin admin = hbaseConnection.getAdmin();
        TableName tableName1 = TableName.valueOf(hbaseNamespace, tableName);
        try {
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        admin.close();
    }

    public static void deleteRow(Connection hbaseConnection, String hbaseNamespace, String sinkTable, String rowKeyValue) throws IOException {
        Table table = hbaseConnection.getTable(TableName.valueOf(hbaseNamespace, sinkTable));
        try {
            table.delete(new Delete(Bytes.toBytes(rowKeyValue)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void putRow(Connection hbaseConnection, String hbaseNamespace, String sinkTable, String sinkFamily, String rowKeyValue, String[] columns, String[] columnValues) throws IOException {
        Table table = hbaseConnection.getTable(TableName.valueOf(hbaseNamespace, sinkTable));
        Put put = new Put(Bytes.toBytes(rowKeyValue));
        for (int i = 0; i < columns.length; i++) {
            if(columnValues[i] ==null){
                columnValues[i] = "";
            }
            put.addColumn(Bytes.toBytes(sinkFamily), Bytes.toBytes(columns[i]), Bytes.toBytes(columnValues[i]));
        }
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        table.close();
    }
}
