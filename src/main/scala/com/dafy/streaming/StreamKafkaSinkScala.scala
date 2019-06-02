package com.dafy.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper

object StreamKafkaSinkScalaScala {
  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    // 连接此socket获取输入数据
    val text = env.socketTextStream("localhost", 10086, '\n')
    val topic = "t1"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("transaction.max.timeout.ms",60000*15+"")


//    val myConsumer = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
//    val text = env.addSource(myConsumer)

   val myProducer =  new FlinkKafkaProducer011[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),prop,FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

    text.addSink(myProducer)
    env.execute(StreamKafkaSinkScalaScala.getClass.getName)


  }

}
