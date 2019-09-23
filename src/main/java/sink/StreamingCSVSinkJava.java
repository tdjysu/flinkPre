package sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import java.util.Properties;


public class StreamingCSVSinkJava {

    public static void main(String args[]) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        //        指定kafka Source
        String intopic = "intent_t1";
        String brokerList = "192.168.8.206:9092,192.168.8.207:9092,192.168.8.207:9092";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",brokerList);
        prop.setProperty("group.id", "con1");
//      设置事务超时时间
        prop.setProperty("transaction.timeout.ms",60000*15+"");
        String outtopic = "t2";
        FlinkKafkaConsumer010 myConsumer =  new FlinkKafkaConsumer010<String>(intopic,new SimpleStringSchema(),prop);

       DataStream<String> resdata = env.addSource(myConsumer);

        String  filePath = "c:/test/kafkadata.txt";
        // 从本地文件读取数据
        DataStream<String> text = env.readTextFile(filePath);


        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(outtopic,new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),prop);


        resdata.addSink(myProducer);
        env.execute(StreamingCSVSinkJava.class.getName());



    }
}
