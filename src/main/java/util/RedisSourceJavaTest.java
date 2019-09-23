package util;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RedisSourceJavaTest {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource  source = env.addSource(new RedisSourceJava());
        source.print();
        env.execute(RedisSourceJavaTest.class.getName());

    }
}
