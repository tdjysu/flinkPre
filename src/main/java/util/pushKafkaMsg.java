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
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;

public class pushKafkaMsg implements Runnable {
//"localhost:9092";
    private final String BROKER_LIST =  "192.168.8.206:9092,192.168.8.207:9092,192.168.8.208:9092";//"192.168.8.206:9092";
    private final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private final String ZK_CONNECT = "192.168.8.206:2181,192.168.8.207:2181,192.168.8.208:2181";//"192.168.8.206:2181";
    private static int Count = 10000;
    private int number = 10;
    Properties props;
    Producer<String, String> producer;

    public pushKafkaMsg() {
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
        for (int i = 0; i < count; i++) {
            String runtime = new Date().toString();

            File file = new File("C:/test/kafkadata.txt");
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                int line = 1;
                JSONObject jsonObject;
                //一次读入一行，直到读入null为文件结束
                while ((tempString = reader.readLine()) != null) {
                    //显示行号

                     jsonObject = JSONObject.parseObject(tempString);
                     String deptcode = jsonObject.getJSONObject("strdeptcode").getString("value");
                     int fundcode = jsonObject.getJSONObject("nborrowmode").getInteger("value");
                     String loandate = jsonObject.getJSONObject("strloandate").getString("value");
                     int nstate = jsonObject.getJSONObject("nstate").getInteger("value");

                    JSONObject newJson = new JSONObject();
                    newJson.put("strdeptcode",deptcode);
                    newJson.put("nborrowmode",fundcode);
                    newJson.put("strloandate",loandate);
                    newJson.put("nstate",nstate);
                    ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, newJson.toJSONString());
                    String msg = "line " + line + ": " + newJson.toJSONString();
                    Future fututre = producer.send(data);
                    fututre.get();
System.out.println("msg = " + msg);
                    Thread.sleep(2000);
                    line++;
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
        }
        producer.close();
    }

    public void run() {
        int j = 0;
        while (j < number) {
            publishMessage("intent_t1", Count);
            number++;
        }
    }
}
