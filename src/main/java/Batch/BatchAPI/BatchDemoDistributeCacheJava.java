package Batch.BatchAPI;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BatchDemoDistributeCacheJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1 注册一个文件,使用本地文件代替
        env.registerCachedFile("C:\\data\\count\\test.txt","testdata");

        DataSource data = env.fromElements("a","b","c","d");
        MapOperator result = data.map(new RichMapFunction<String,String>() {
            private ArrayList dataList = new ArrayList<String>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
//          2 使用文件
                File myfile = getRuntimeContext().getDistributedCache().getFile("testdata");
                List<String>  lines = FileUtils.readLines(myfile);
                for(String word:lines){
                    this.dataList.add(word);
                    System.out.println("line-->" + word);
                }

            }


            @Override
            public String map(String value) throws Exception {
//              在这里就可以使用dataList
                return value;
            }
        });

       result.print();


    }


}
