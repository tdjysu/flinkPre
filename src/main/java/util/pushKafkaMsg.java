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

            File file = new File("C:/test/UPDATE.json");
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = reader.readLine();
                int line = 1;
                JSONObject jsonObject;
                DateFormat df= new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
//              保存已插入kafka的数据记录
                Map<String,DataBean> beforeRecordMap = new HashMap<String, DataBean>();

                //一次读入一行，直到读入null为文件结束
                while (line <= 100000) {
                    //解析文本生成Json数据
                    JSONObject newJson = getJsonObject(tempString, df,beforeRecordMap);
                    //生成kafka生产数据
                    ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, newJson.toJSONString());
                    String msg = "line " + line + ": " + newJson.toJSONString();
                    //将数据发送至kafka
                    Future fututre = producer.send(data);
                    fututre.get();
System.out.println("msg = " + msg);
                    Thread.sleep(1000);




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

    //解析文本生成json
    private JSONObject getJsonObject(String tempString, DateFormat df,Map<String,DataBean> beforeRecordMap) throws ParseException {

        String intentID = new Random().nextInt(100000)+"";
        String deptcode = getRandomDept();
        int fundcode = getRandomFund();
        Date loandate = new Date();
        int nstate = getRandomState();
        int userid = randomInt(7);
        int lamount = randomInt(4)*100;
        String opFlag = "I";//jsonObject.getString("opFlag");
        JSONObject beforeDataJson = new JSONObject();
        JSONObject newJson = new JSONObject();
        JSONObject oldJson = new JSONObject();
        boolean isUpdate = false;
        if(beforeRecordMap.size() >= 10){//若已插入数据超过50条,则开启更新数据插入
            String[] keys = {"I","U","D","Q","C"};
            LocalDateTime curTime = LocalDateTime.now();

            if ("U".equals(keys[new Random().nextInt(4)])){
                isUpdate = true;
                opFlag = "U";
            }
        }

        if(isUpdate){//若是更新数据,则从已插入数据集合中取出旧数据拼装,并生成更新数据
            int mapLength = beforeRecordMap.size()-1;
            String keystr = beforeRecordMap.keySet().toArray()[new Random().nextInt(mapLength)].toString();
            intentID = keystr;
            DataBean beforeData = beforeRecordMap.get(intentID);
            newJson.put("intentID",intentID);//新数据不修改
            newJson.put("strdeptcode",beforeData.getDeptCode());//新数据不修改
            newJson.put("nborrowmode",beforeData.getFundcode());//新数据不修改
            newJson.put("strloandate",beforeData.getLoandate());//新数据不修改
//                    newJson.put("strloandate",loandate);
            newJson.put("nstate",nstate);
            newJson.put("userid",beforeData.getUserid());//新数据不修改
            newJson.put("lamount",lamount);
            newJson.put("opFlag",opFlag);


            //          根据解析后的数据生成临时数据Bean
            DataBean currentDataBean = new DataBean();
            currentDataBean.setIntentId(intentID);
            currentDataBean.setDeptCode(beforeData.getDeptCode());
            currentDataBean.setFundcode(beforeData.getFundcode());
            currentDataBean.setNstate(nstate);
            currentDataBean.setUserid(beforeData.getUserid());
            currentDataBean.setLamount(lamount);
            currentDataBean.setLoandate(beforeData.getLoandate());
//          将数据Bean存入已插入数据集合中
            beforeRecordMap.put(currentDataBean.getIntentId(),currentDataBean);

            oldJson.put("intentID",intentID);
            oldJson.put("strdeptcode",beforeData.getDeptCode());
            oldJson.put("nborrowmode",beforeData.getFundcode());
            oldJson.put("strloandate",beforeData.getLoandate());
            oldJson.put("nstate",beforeData.getNstate());
            oldJson.put("userid",beforeData.getUserid());
            oldJson.put("lamount",beforeData.getLamount());
            newJson.put("beforeRecord",oldJson);

            return newJson;
        }else {//若是插入数据,则生成新的数据


            newJson.put("intentID",intentID);
            newJson.put("strdeptcode",deptcode);
            newJson.put("nborrowmode",fundcode);
            newJson.put("strloandate",loandate);
//                    newJson.put("strloandate",loandate);
            newJson.put("nstate",nstate);
            newJson.put("userid",userid);
            newJson.put("lamount",lamount);
            newJson.put("opFlag",opFlag);
            newJson.put("beforeRecord",beforeDataJson);
//          根据解析后的数据生成临时数据Bean
            DataBean currentDataBean = new DataBean();
            currentDataBean.setIntentId(intentID);
            currentDataBean.setDeptCode(deptcode);
            currentDataBean.setFundcode(fundcode);
            currentDataBean.setNstate(nstate);
            currentDataBean.setUserid(userid);
            currentDataBean.setLamount(lamount);
            currentDataBean.setLoandate(loandate);
//          将数据Bean存入已插入数据集合中
            beforeRecordMap.put(currentDataBean.getIntentId(),currentDataBean);
        }




        return newJson;
    }

    public static int randomInt(int size) {
        Random random = new Random();
        if(size < 2){
            return random.nextInt(10);
        }
        Double pow = Math.pow(10, size - 1);
        int base = pow.intValue();
        int number = base + random.nextInt(base * 9);
        return number;
    }

    public static String getRandomDept(){
        String deptcode = "";
        String[] deptArray = {"114523201","012122931","115327801","021302456","033222102","011516832",
                "012313390","021302836","012101956","021437832","033417601","034222031","021204816",
                "021315681","041303546","115140231","012100111","012101846","034201701","021417762",
                "026122522","012112430","012113560","035305096","041303436","041304566","023734612",
                "033323071","041303816","033501711"};

        String[] deptArray2 = {"114523201","012122931","115327801"};

        deptcode = deptArray2[new Random().nextInt(3)];
        return deptcode;
    }


    public static int getRandomFund(){
        int strFund ;
        int[] fundArray = {0,9,12,15,18,20};
        strFund = fundArray[new Random().nextInt(6)];
        return strFund;
    }

    public static int getRandomState(){
        int strState ;
        int[] stateArray = {4,5,7,9,32,8};
        strState = stateArray[new Random().nextInt(6)];
        return strState;
    }
    public void run() {
        int j = 0;
        while (j < number) {
            publishMessage("intent_t1", Count);
            number++;
        }
    }
}
