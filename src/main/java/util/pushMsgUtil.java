package util;

public class pushMsgUtil {

    public static void main(String[] args) throws Exception{

        int logtype = 2;// 0 原生意向日志 1 拆分后意向日志 2 平台点击日志


        if(logtype == 0) {
            pushKafkaMsg pushmsg = new pushKafkaMsg();
            String topic = "intent_t1";
            int recordCnt = 10000;
            pushmsg.publishMessage(topic, recordCnt);
        }else if (logtype == 1) {
            pushIntentMsg pushIntent = new pushIntentMsg();
            String topic = "intent_n1";
            int recordCnt = 10000;
//            int threadNum = 5;
//            int i = 0;
//            while ( i <= threadNum){

//                new Thread(pushIntent).start();
//            }
            pushIntent.publishMessage(topic, recordCnt);
        } else if(logtype == 2){
            String topic = "athena_t1";
            PushFuncLogMsg pushMsg = new PushFuncLogMsg(topic);
            pushMsg.publishMessage(10000);
//            int threadNum = 1;
//            int i = 0;
//            while (i < threadNum){
//                new Thread(pushMsg).start();
//                i++;
//            }
        }
    }
}
