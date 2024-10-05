import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author rock
 * @date 2024/9/29 16:01
 */
public class FlinkKafka {
    public static void main(String[] args) throws Exception {

        // create env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // create kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092")
                .setTopics("topic_db")
                .setGroupId("flink_kafka")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .build();

        // read kafka
        DataStreamSource<String> flinkKafka = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "flink_kafka");
        flinkKafka.print("flink_kafka>>>");

        // execute task
        env.execute();
    }
}
