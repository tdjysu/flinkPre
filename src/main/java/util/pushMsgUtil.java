package util;

public class pushMsgUtil {

    public static void main(String[] args){
        pushKafkaMsg pushmsg = new pushKafkaMsg();
        String topic = "t1";
        int recordCnt = 10000;
        pushmsg.publishMessage(topic,recordCnt);
    }
}
