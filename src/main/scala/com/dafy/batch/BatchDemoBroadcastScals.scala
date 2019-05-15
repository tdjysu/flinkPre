package com.dafy.batch

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * 广播变量
  */

object BatchDemoBroadcastScals {
  def main(args: Array[String]): Unit = {
   val  env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

// 1.准备要广播的数据
    val broadcastdata = ListBuffer[Tuple2[String,Int]]()
    broadcastdata.append(("UK",80))
    broadcastdata.append(("USA",90))
    broadcastdata.append(("CN",99))

//    处理需要广播的数据

     val tupleData = env.fromCollection(broadcastdata)

     val tuBroadcastdata = tupleData.map(tup=>{
      Map(tup._1->tup._2)
    })
// 准备数据源
    val text = env.fromElements("UK","USA","CN")

    val result = text.map(new RichMapFunction[String,String] {
//      从广播中获取到的数据，注意List类型加上包的全路径 java.util.List
      var listdata: java.util.List[Map[String,Int]] = null
//      将获取到的数据装入Map
      var allMap = Map[String,Int]()

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
//        此处注意返回的数据类型，增加了范型,约束返回值类型
        this.listdata = getRuntimeContext.getBroadcastVariable[Map[String,Int]]("boradcastMapName")
        val it = listdata.iterator()
        while (it.hasNext){
          val next = it.next()
          allMap = allMap.++(next) //遍历返回结果生成Map
        }
      }
      override def map(value: String): String = {
//        注意最后的get方法,多种scala中Map获取值的方法
//          val score = allMap.get(value).get
        val score = allMap.getOrElse(value,-1)
        value + "-->" + score
      }
    }).withBroadcastSet(tuBroadcastdata,"boradcastMapName")

    result.print()
  }
}
