import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author rock
 * @date 2024/9/29 15:29
 */
public class FlinkCDCMysql {

    public static void main(String[] args) throws Exception{
        // create evn
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // create mysql source
        MySqlSource<String> mysqlSource = MySqlSource.<String>builder()
                .hostname("192.168.125.100")
                .port(3306)
                .databaseList("financial_lease_config")
                .tableList("financial_lease_config.table_process")
                .username("root")
                .password("hadoop")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        //print data
        env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(),"flink_cdc").print("flink_cdc>>>");

        // execute task
        env.execute();
    }
}

