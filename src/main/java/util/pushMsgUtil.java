package util;

import java.util.Random;

public class pushMsgUtil {

    public static void main(String[] args) throws Exception{
        boolean isOrginal = true;
        if(!isOrginal) {
            pushKafkaMsg pushmsg = new pushKafkaMsg();
            String topic = "intent_t1";
            int recordCnt = 10000;
            pushmsg.publishMessage(topic, recordCnt);
        }else {
            pushIntentMsg pushIntent = new pushIntentMsg();
            String topic = "intent_n1";
            int recordCnt = 10000;
//
//            int threadNum = 5;
//            int i = 0;
//            while ( i <= threadNum){

//                new Thread(pushIntent).start();
//            }
            pushIntent.publishMessage(topic, recordCnt);
        }
    }
}
