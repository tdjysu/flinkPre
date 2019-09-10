package util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.Properties;

public class pushKafkaMsg implements Runnable {
    private static final String TOPIC = "kafka_topicconfig_test_1";

    private final String BROKER_LIST = "localhost:9092";
    private final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder";
    private final String ZK_CONNECT = "localhost:2181";
    private static int Count = 10000;
    private int number = 10;
    Properties props;
    Producer<String, String> producer;

    public pushKafkaMsg() {
        props = new Properties();
        props.put("zk.connect", ZK_CONNECT);
        props.put("serializer.class", SERIALIZER_CLASS);
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("request.required.acks", "1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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
                //一次读入一行，直到读入null为文件结束
                while ((tempString = reader.readLine()) != null) {
                    //显示行号
                    String msg = "line " + line + ": " + tempString;
                    ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, msg);
                    producer.send(data);
System.out.println("msg = " + msg);
                    Thread.sleep(100);
                    line++;
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
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
            publishMessage(TOPIC, Count);
            number++;
        }
    }
}
