package util;

import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * @ClassName DataCheckTest
 * @Description:TODO
 * @Author Albert
 * Version v0.9
 */
public class DataCheckTest {
     public static void main(String[] args) {

         File file = new File("C:/tmp/02.csv");
         BufferedReader  reader = null;

         try{
            reader = new BufferedReader(new FileReader(file));
            String line = "";
            while ((line = reader.readLine()) != null){
                JSONObject jsonObject = JSONObject.parseObject(line);
                if(jsonObject != null){
                    String opFlag = jsonObject.getString("opFlag");
                    String intentId = jsonObject.getJSONObject("lid").getString("value");
                    String deptCode = jsonObject.getJSONObject("strdeptcode").getString("value");
                    String loandate = jsonObject.getJSONObject("strloandate").getString("value");
                    String fundcode = jsonObject.getJSONObject("nborrowmode").getString("value");
                    String lamount = jsonObject.getJSONObject("lamount").getString("value");
                    String intentState = jsonObject.getJSONObject("nstate").getString("value");
                    String opTime = jsonObject.getJSONObject("dtmodifytime").getString("value");


                    JSONObject beforeRecord = jsonObject.getJSONObject("beforeRecord");
                    String oldDeptCode = "";
                    String oldIntentId = "";
                    String oldLoandate = "";
                    String oldFundcode = "";
                    String oldLamount = "";
                    String oldIntentState = "";
                    if(beforeRecord != null && beforeRecord.size() > 0){
                        oldIntentId = beforeRecord.getJSONObject("lid").getString("value");
                        oldDeptCode = beforeRecord.getJSONObject("strdeptcode").getString("value");
                        oldLoandate = beforeRecord.getJSONObject("strloandate").getString("value");
                        oldFundcode = beforeRecord.getJSONObject("nborrowmode").getString("value");
                        oldLamount = beforeRecord.getJSONObject("lamount").getString("value");
                        oldIntentState = beforeRecord.getJSONObject("nstate").getString("value");
                    }

System.out.println(opFlag+","+intentId+","+deptCode+","+loandate+","+fundcode+","+lamount+","+intentState+","+opTime);
System.out.println("before"+","+oldIntentId+","+oldDeptCode+","+oldLoandate +","+oldFundcode+","+oldLamount+","+oldIntentState);

                }
            }

            reader.close();


         }catch (Exception e){
             e.printStackTrace();
         }
    }
}
