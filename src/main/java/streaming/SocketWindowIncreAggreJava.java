package streaming;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;


public class SocketWindowIncreAggreJava {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "localhost";
        int port = 10086;
        String delimiter = "\n";

        DataStreamSource<String> text = env.socketTextStream(hostname,port,delimiter);

        DataStream<Tuple2<Integer,Integer>> inputData =  text.map(new MapFunction<String, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                return new Tuple2<>(1,Integer.parseInt(value));
            }
        });

        inputData.keyBy(0)
                .timeWindow(Time.seconds(3))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
                        System.out.println("Reduce Processing " + value1 + "," + value2);
                        return new Tuple2<>(value1.f0,value1.f1 + value2.f1);
                    }
                }).print();

        env.execute(SocketWindowIncreAggreJava.class.getName());




    }
}
