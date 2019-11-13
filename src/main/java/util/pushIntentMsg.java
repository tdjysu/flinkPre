package util;



import DataBean.DataBean;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Future;

public class pushIntentMsg implements Runnable {
//"localhost:9092";
    private final String BROKER_LIST =  "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";//"192.168.8.206:9092";
    private final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private final String ZK_CONNECT = "192.168.8.206:2181,192.168.8.207:2181,192.168.8.208:2181";//"192.168.8.206:2181";
    private static int Count = 10000;
    private int number = 10;
    Properties props;
    Producer<String, String> producer;

    public pushIntentMsg() {
        props = new Properties();
        props.put("zk.connect", ZK_CONNECT);
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("group.id", "CountryCounter");
        props.put("request.required.acks", "1");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<String, String>(props);
    }

    public void publishMessage(String topic, int count) {

            String runtime = new Date().toString();

            File file = new File("C:/test/intent_data");
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                int line = 1;
                DateFormat df= new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

                //一次读入一行，直到读入null为文件结束
                while ( (tempString = reader.readLine()) != null) {
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
                    Thread.sleep(20);
                }
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                    }
                }
            }
        producer.close();

        }


    public void run() {
        int j = 0;
        publishMessage("intent_n1", Count);
    }


}
