package Batch.BatchAPI;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * 如果多个算子需要使用同一个数据集，需要为每一个算子注册一个广播变量
 */

public class BatchDemoBroadcastJava {
    public static  void main(String[] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//      1 准备要广播的数据
        ArrayList<Tuple2<String,Integer>> broadData = new ArrayList<>();
        broadData.add(new Tuple2<>("UK",88));
        broadData.add(new Tuple2<>("USA",90));
        broadData.add(new Tuple2<>("CN",99));
        DataSet<Tuple2<String,Integer>> tupleData = env.fromCollection(broadData);


//      2 处理需要广播的数据，把数据转换成Map类型，map中的key就是用户名，value就是数值
        DataSet<HashMap<String,Integer>> toBroadcase= tupleData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String,Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String,Integer> res = new HashMap<>();
                res.put(value.f0,value.f1);
                return res;
            }
        });

//       3源数据
        DataSource<String> srcdata = env.fromElements("UK","USA","CN");

        DataSet<String> result = srcdata.map(new RichMapFunction<String,String>() {
             List<HashMap<String,Integer>> broadcastMap =  new ArrayList<HashMap<String,Integer>>();
             HashMap<String,Integer> allMap = new HashMap<String,Integer>();
//           这个方法只执行一次，可以在这里执行初始化功能
//           所以可以在open 方法中获取广播变量的数据,即在一次获取到广播变量的数据

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//        4 获取广播数据
                  this.broadcastMap = getRuntimeContext().getBroadcastVariable("broadcaseMapName");
                  for(HashMap map:broadcastMap){
                      allMap.putAll(map);
                  }
            }

            @Override
            public String map(String value) throws Exception {
                Integer score = allMap.get(value);
                return value+ "-->"+score ;
            }
        }).withBroadcastSet(toBroadcase,"broadcaseMapName");//3执行广播数据的操作

       result.print();
    }
}
