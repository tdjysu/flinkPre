package com.dafy.batch

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoJoinScala {
  def main(args: Array[String]): Unit = {
    val  env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val data1 = ListBuffer[Tuple2[Int,String]]()
    data1.append((1,"Tom"))
    data1.append((2,"Jerry"))
    data1.append((3,"Albert"))
    val text1 = env.fromCollection(data1)

    val data2 = ListBuffer[Tuple2[Int,String]]()
    data2.append((1,"USA"))
    data2.append((2,"UK"))
    data2.append((3,"CN"))
    val text2 = env.fromCollection(data2)

    text1.join(text2).where(0).equalTo(0).apply((first,second) => {
      (first._1,first._2,second._2)
    }).sortPartition(1,Order.ASCENDING).print()

    text1.join(text2).where(0).equalTo(0).map((rs) => {
      (rs._1._1,rs._1._2,rs._2._2)
    }).sortPartition(_._2,Order.ASCENDING).print()
  }

}
