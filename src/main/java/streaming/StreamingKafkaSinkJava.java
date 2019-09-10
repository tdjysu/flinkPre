package streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;


public class StreamingKafkaSinkJava {
    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//      checkpoint配置
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> text = env.socketTextStream("localhost",8686,"\n");
        String brokerList = "localhost:9092";
        String topic = "t1";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
//      设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");
//      方案2修改kafka transaction.max.timeout.mx=360000

//       使用至少一次语义(默认值)
//        FlinkKafkaProducer011<String> myProducer =  new FlinkKafkaProducer011<>(brokerList,topic,new SimpleStringSchema());
//        使用仅一次语义
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(topic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        text.addSink(myProducer);
        env.execute(StreamingKafkaSinkJava.class.getName());
    }
}
