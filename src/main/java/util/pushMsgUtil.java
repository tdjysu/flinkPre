package util;

import java.util.Random;

public class pushMsgUtil {

    public static void main(String[] args) throws Exception{
        pushKafkaMsg pushmsg = new pushKafkaMsg();
        String topic = "intent_t1";
        int recordCnt = 10000;
        pushmsg.publishMessage(topic,recordCnt);

    }
}
