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
                Map beforeRecordMap = new HashMap<String, DataBean>();

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
    private JSONObject getJsonObject(String tempString, DateFormat df,Map beforeRecordMap) throws ParseException {


        if(beforeRecordMap.size() >= 50){//若已插入数据超过50条,则开启更新数据插入
            String[] keys = {"I","U","D","Q","C"};
            LocalDateTime curTime = LocalDateTime.now();
            boolean isUpdate = false;
            if ("U".equals(new Random().nextInt(4))){
                isUpdate = true;
            }



        }

        JSONObject jsonObject;
        jsonObject = JSONObject.parseObject(tempString);
        String intentID = new Random().nextInt(100000)+"";
        String deptcode = jsonObject.getJSONObject("strdeptcode").getString("value");
        int fundcode = jsonObject.getJSONObject("nborrowmode").getInteger("value");
        Date loandate = df.parse(jsonObject.getJSONObject("strloandate").getString("value"));
        int nstate = jsonObject.getJSONObject("nstate").getInteger("value");
        int userid = jsonObject.getJSONObject("lborrowerid").getInteger("value");
        int lamount = jsonObject.getJSONObject("lamount").getInteger("value");
        String opFlag = "I";//jsonObject.getString("opFlag");
        JSONObject beforeData = jsonObject.getJSONObject("beforeRecord");
        JSONObject newJson = new JSONObject();
        newJson.put("strdeptcode",getRandomDept());
        newJson.put("nborrowmode",getRandomFund());
        newJson.put("strloandate",new Date());
//                    newJson.put("strloandate",loandate);
        newJson.put("nstate",getRandomState());
        newJson.put("userid",randomInt(7));
        newJson.put("lamount",lamount);
        newJson.put("opFlag",opFlag);

        DataBean currentDataBean = new DataBean();
        currentDataBean.setIntentId(intentID);
        currentDataBean.setDeptCode(deptcode);
        currentDataBean.setFundcode(fundcode);
        currentDataBean.setNstate(nstate);
        currentDataBean.setUserid(userid);
        currentDataBean.setLamount(lamount);
        currentDataBean.setLoandate(loandate);

        beforeRecordMap.put(currentDataBean.getIntentId(),currentDataBean);
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


    public static String getRandomFund(){
        String strFund = "";
        String[] fundArray = {"0","9","12","15","18","20"};
        strFund = fundArray[new Random().nextInt(6)];
        return strFund;
    }

    public static String getRandomState(){
        String strState = "";
        String[] stateArray = {"4","5","7","9","32","8"};
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
