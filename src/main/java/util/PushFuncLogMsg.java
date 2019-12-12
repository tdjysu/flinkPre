package util;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class PushFuncLogMsg implements Runnable {
//"localhost:9092";
    private final String BROKER_LIST =  "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";//"192.168.8.206:9092";
    private final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private final String ZK_CONNECT = "192.168.8.206:2181,192.168.8.207:2181,192.168.8.208:2181";//"192.168.8.206:2181";
    private static int Count = 10000;
    private int number = 10;
    private String topic = "";
    Properties props;
    Producer<String, String> producer;

    public PushFuncLogMsg(String topic ) {
        props = new Properties();
        props.put("zk.connect", ZK_CONNECT);
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "CountryCounter");
        props.put("request.required.acks", "1");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.topic = topic;
        producer = new KafkaProducer<String, String>(props);
    }

    public void publishMessage( int count) {

            String runtime = new Date().toString();
            DateFormat df= new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
            try {
                String opDate = df.format(new Date());
                int line = 1;


                while ( line <= count) {
                    String tempString = "{" +
                            "  \"appId\": \"datacube_athena\"," +
                            "  \"bossUserId\": \"" +getRandomUserID()+"\"," +
                            "  \"errorMsg\": \"500ERROR\"," +
                            "  \"funcId\": \""+getRandomFuncID()+"\"," +
                            "  \"funcName\": \"营业部日利润数据统计\"," +
                            "  \"logType\": 1," +
                            "  \"opDate\": \""+opDate+"\"," +
                            "  \"opName\": \"logOperation.value(),注解上的值\"," +
                            "  \"orgCode\": \""+getRandomDept()+"\"," +
                            "  \"orgLevel\": 10," +
                            "  \"orgName\": \"大数据中心\"," +
                            "  \"requestInput\": \"JSON.toJSONString(request.getParameterMap()),参数列表\"," +
                            "  \"requestUrl\": \"http://prepare.xdata.dafy.com/athena/report/lend/lenddetail/query4LendDataUpdateTime\"," +
                            "  \"srcIp\": \"\"," +
                            "  \"success\": true," +
                            "  \"supOrgCode\": \"991500000\"," +
                            "  \"supOrgName\": \"云扬达飞\"," +
                            "  \"userId\": \""+getRandomUserID()+"\"," +
                            "  \"userName\": \"张三\"" +
                            "}";
                    //解析文本生成Json数据
                        JSONObject json = JSONObject.parseObject(tempString);
                        //生成kafka生产数据
                        ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, json.toJSONString());
                        String msg = "line " + line + ": " + json.toJSONString();
                        //将数据发送至kafka
                        Future fututre = producer.send(data);
                        fututre.get();
//System.out.println("msg = " + msg);
                        line++;
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        producer.close();

        }

    public static String getRandomFuncID(){
        String strFuncID ;
        String[] fundArray = {"01b7e04e7bf340c49f1de528411a5ed5","023ba020d6424991b05c570d204f80ea","029f3df98f814f47a3c5b4f9fab332b6",
                              "05629eff9bd24b08a904db33aab956a9","07757d3018024e83a2c52c4d965328dd","0a7dc9793382449597d92f3580b1c5cd",
                              "0b4be81243f14ef4a7993390a56ede5a","0c5f7bc01b1549cb87e89f8f12dd8198"};
        strFuncID = fundArray[new Random().nextInt(8)];
        return strFuncID;
    }

    public static String getRandomUserID(){
        String strUserD ;
        String[] userArray = {"10135","10138","99d3cb7c61d84f3f952e95a5358d5bd5","7812dccbd6d648d2b6a385301dce1c30","bf4a3cd8926f46259cc160d0d4a5ce5b",
                             "0d4cfd3d7b614049b558c7ced997e867","477dbb5cf3ec4a17a302a1b5292584de","fb98af38ac834372af731f58022d9c33"};
        strUserD = userArray[new Random().nextInt(8)];
        return strUserD;
    }

    public static String getRandomDept(){
        String deptcode = "";
        String[] deptArray = {"114523201","012122931","115327801","021302456","033222102","011516832",
                "012313390","021302836","012101956","021437832","033417601","034222031","021204816",
                "021315681","041303546","115140231","012100111","012101846","034201701","021417762",
                "026122522","012112430","012113560","035305096","041303436","041304566","023734612",
                "033323071","041303816","033501711"};
        deptcode = deptArray[new Random().nextInt(30)];
        return deptcode;
    }
    @Override
    public void run() {
        try {
            Thread.sleep(1000);
            publishMessage(Count);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


}
