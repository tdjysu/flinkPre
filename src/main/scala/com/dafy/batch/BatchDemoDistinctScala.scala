package com.dafy.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

object BatchDemoDistinctScala {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val data =  ListBuffer[String]()
    data.append("Hello USA")
    data.append("Hello TW")

    val text = env.fromCollection(data)

    val flatMapData = text.flatMap( line => {
      val words  = line.split("\\W+")
      for(word<- words){
        println("The word is "+word)
      }
      words
    })

     flatMapData.distinct().print()
  }
}
