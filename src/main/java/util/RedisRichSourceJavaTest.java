package util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisRichSourceJavaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.addSource(new MyRedisSource());
        source.print();
        env.execute("redis  source");
    }
    //自定义source
    public static class MyRedisSource extends RichSourceFunction<String> {
        //获取连接池的配置对象
        private JedisPoolConfig config = null;
        //获取连接池
        JedisPool jedisPool = null;
        //获取核心对象
        Jedis jedis = null;
        //Redis服务器IP
        private static String ADDR = "localhost";
        //Redis的端口号
        private static int PORT = 6379;
        //访问密码
        private static String AUTH = "xxxxxx";
        //等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
        private static int TIMEOUT = 10000;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            config = new JedisPoolConfig();
//            jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT, AUTH);
            jedis = new Jedis(ADDR,PORT);
        }

        /**
         * 启动一个 source，即对接一个外部数据源然后 emit 元素形成 stream
         * （大部分情况下会通过在该方法里运行一个 while 循环的形式来产生 stream）。
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
            String pv = jedis.hget("areas", "AREA_US");
            ctx.collect(pv);
        }

        /**
         * 取消源，大多数源文件都有一个while循环
         * {@link #run(SourceContext)}方法。实现需要确保
         * 调用此方法后，source将跳出该循环。
         */
        @Override
        public void cancel() {
        }

        /**
         * 它是在最后一次调用主工作方法之后调用的, 此方法可用于清理工作
         */
        @Override
        public void close() throws Exception {
            super.close();
//            jedisPool.close();
            jedis.close();
        }
    }

}
