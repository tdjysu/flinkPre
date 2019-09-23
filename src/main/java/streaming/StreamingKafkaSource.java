package streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;


import java.util.Properties;

public class StreamingKafkaSource {
    public static void main(String[] args) throws Exception{

//      1 打开zookeeper zkserver
//      2 运行kafka .\bin\windows\kafka-server-start.bat .\config\server.properties
//      3 创建主题t1 bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic t1
//      4 查看主题 bin\windows\kafka-topics.bat --zookeeper 127.0.0.1:2181 --list
//      5 创建生产者 bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic t1
//      6 创建消费者 bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic t1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        设置statebackend


//kafka 0.9，0.10不支持只消费一次，0.11支持只消费一次
        String topic = "t1";

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.8.206:9092");
        prop.setProperty("group.id","con1");
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<String>(topic,new SimpleStringSchema(),prop);
        myConsumer.setStartFromGroupOffsets();//默认消费策略
        DataStreamSource<String> text = env.addSource(myConsumer);
        text.print();
        env.execute(StreamingKafkaSource.class.getName());
    }
}
