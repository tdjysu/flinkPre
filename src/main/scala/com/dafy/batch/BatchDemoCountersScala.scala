package com.dafy.batch

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BatchDemoCountersScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment;
    import org.apache.flink.api.scala._

//    1数据源准备
    val data = env.fromElements("a","b","c","d")
    val result = data.map(new RichMapFunction[String,String] {

//      创建累加器
      val numlines = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
//注册累加器
        getRuntimeContext.addAccumulator("numlies",this.numlines)
      }

      override def map(value: String): String = {
          this.numlines.add(1)
        value
      }
    }).setParallelism(8)

    result.writeAsText("c:\\data\\count")

    val jobResult = env.execute("counterJob")
//    注意约束返回值类型
    val num = jobResult.getAccumulatorResult[Int]("numlies")
    println("num-->" + num)
  }
}
