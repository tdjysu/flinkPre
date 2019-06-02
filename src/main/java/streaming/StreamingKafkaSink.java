package streaming;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


public class StreamingKafkaSink {
    public static void main(String[] args) throws Exception{


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> text = env.socketTextStream("localhost",8686,"\n");
        String brokerList = "localhost:9092";
        String topic = "t1";
        FlinkKafkaProducer011<String> myProducer =  new FlinkKafkaProducer011<>(brokerList,topic,new SimpleStringSchema());
        text.addSink(myProducer);
        env.execute(StreamingKafkaSink.class.getName());
    }
}
