package Batch.BatchAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;


/**
 * 全局累加器
 * 计算map函数计算了多少数据
 * 注意：只有在任务执行结束后，才能获取到累加器的值
 */
public class BatchDemoCountersJava {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        1准备数据源
        DataSource<String> data = env.fromElements("a","b","c","d");

        DataSet result = data.map(new RichMapFunction<String,String>() {

//1 创建累加器
            private  IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {

                super.open(parameters);
// 2 注册累加器
             getRuntimeContext().addAccumulator("num+lines",this.numLines);

            }
//普通的累加器
//            int sum = 0;

            @Override
            public String map(String value) throws Exception {
//              如果并行度为1，使用普通的累加器可以正常
//                若并行度大于1，侧普通的累加器不可以使用
//                sum++;
//                System.out.println("sum->" + sum);
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(8);

//        result.print(); 不能指定 print 做为sink
//        需要指定写数据的sink
        result.writeAsText("c:\\data\\count");
//      需要在任务执行之后的返回值中获取累加器的结果
        JobExecutionResult jobresult = env.execute("counterJob");
        int num = jobresult.getAccumulatorResult("num+lines");
        System.out.println("num->"+ num);


    }
}
