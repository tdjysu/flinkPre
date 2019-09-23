package util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import redis.clients.jedis.Jedis;
import java.util.HashMap;

public class RedisSourceJava implements SourceFunction<HashMap<String, String>> {


    private static String host = "localhost";
    private static int port = 6379;
    private static Jedis myJedis = new Jedis(host,port);
    @Override
    public void run(SourceContext ctx) throws Exception {
        String pv = myJedis.hget("areas","AREA_US");
        ctx.collect(pv);
    }

    @Override
    public void cancel() {
        if(myJedis != null){
            myJedis.close();
        }
    }
}
