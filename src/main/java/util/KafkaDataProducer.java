package util;


import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class KafkaDataProducer {
    public static void main(String[] args) throws  Exception{
    String filePath = "c:/test/intentKafkaData.txt";
    String rs = KafkaDataProducer.readJsonData(filePath);
    JSONObject srcObject = JSONObject.parseObject(rs);
    KafkaDataProducer.writeJsonData(KafkaDataProducer.geneRandomData(srcObject));

    }

    /**
     * 读取JSON文件
     * @param filePath
     * @return
     * @throws IOException
     */
    public static String readJsonData(String filePath) throws IOException {
        // 读取文件数据
        //System.out.println("读取文件数据util");

        StringBuffer strbuffer = new StringBuffer();
        File myFile = new File(filePath);//"D:"+File.separatorChar+"DStores.json"
        if (!myFile.exists()) {
            System.err.println("Can't Find " + filePath);
        }
        try {
            FileInputStream fis = new FileInputStream(filePath);
            InputStreamReader inputStreamReader = new InputStreamReader(fis, "UTF-8");
            BufferedReader in  = new BufferedReader(inputStreamReader);

            String str;
            while ((str = in.readLine()) != null) {
                strbuffer.append(str);  //new String(str,"UTF-8")
            }
            in.close();
        } catch (IOException e) {
            e.getStackTrace();
        }
        //System.out.println("读取文件结束util");
        return strbuffer.toString();
    }


    public static void writeJsonData(String jsonStr) throws Exception{
        try {
            File file = new File("c:/test/kafkadata.txt");

            // if file doesnt exists, then create it
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(jsonStr);
            bw.close();

            System.out.println("Done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 生成随机意向JSON字符串
     * @param jsonObject
     * @return
     * @throws Exception
     */
    public static String geneRandomData(JSONObject jsonObject) throws Exception{
      StringBuffer finalData = new StringBuffer();
      int lincnt = 10000;
      String strloandate = "";
      String strdeptcode = "";
      int lamount = 0;
      List<String> rslist = new ArrayList<String>();

      for( int i = 0;i<lincnt;i++){
          strloandate = KafkaDataProducer.getRandomDate();
          strdeptcode = "0"+KafkaDataProducer.getRandomNum(8);
          lamount =  KafkaDataProducer.getRandomNum(7);

          JSONObject newjson = new JSONObject();
          newjson.putAll(jsonObject);
          newjson.getJSONObject("strloandate").put("value",strloandate);
          newjson.getJSONObject("strdeptcode").put("value",strdeptcode);
          newjson.getJSONObject("lamount").put("value",lamount);
          rslist.add(newjson.toString());
          finalData.append(newjson.toString()+"\r\n");
      }

      /*for (String str:rslist){
          JSONObject nxtJson = JSONObject.parseObject(str);
          System.out.println("strloandate-->" + nxtJson.getJSONObject("strloandate").getString("value")
                  + " strdeptcode" + nxtJson.getJSONObject("strdeptcode").getString("value")
                  + " lamount" + nxtJson.getJSONObject("lamount").getBigInteger("value")

          );
      }*/
          return finalData.toString();


    }

    /**
     * 生成随机日期
     * @return
     */
    public static  String getRandomDate() {
        Random rndYear = new Random();
        int year = rndYear.nextInt(3) + 2017;  //生成[2017,2019]的整数；年
        Random rndMonth = new Random();
        int month = rndMonth.nextInt(12) + 1;   //生成[1,12]的整数；月
        Random rndDay = new Random();
        int Day = rndDay.nextInt(30) + 1;       //生成[1,30)的整数；日
        Random rndHour = new Random();
        int hour = rndHour.nextInt(23);       //生成[0,23)的整数；小时
        Random rndMinute = new Random();
        int minute = rndMinute.nextInt(60);   //生成分钟
        Random rndSecond = new Random();
        int second = rndSecond.nextInt(60);   //秒
        return year + "-" + month + "-" + Day + "  " + hour + ":" + minute + ":" + second;
    }

    /**
     * @param digit 位数
     * @return 随机生成digit位数的数字
     */
    public static int getRandomNum(int digit) {
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < digit; i++) {
            if (i == 0 && digit > 1)
                str.append(new Random().nextInt(9) + 1);
            else
                str.append(new Random().nextInt(10));
        }
        return Integer.valueOf(str.toString());
    }
 }


