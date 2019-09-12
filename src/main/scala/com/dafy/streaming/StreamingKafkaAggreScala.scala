package com.dafy.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object StreamingKafkaAggreScala {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val topic = "t1"
    val prop = new Properties()
    prop.setProperty("bootstrap.server","loalhost:9092")
    prop.setProperty("group.id","con1")
    val myConsumer = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),prop)
    val text = env.addSource(myConsumer)
    text.print()

  }

}
