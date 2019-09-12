package com.dafy.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
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
    //启动zookeeper zkserver.cmd
    //启动kafka  .\bin\windows\kafka-server-start.bat .\config\server.properties
    //查看topic .\bin\windows\kafka-topics.bat --list --zookeeper localhost:2181
    //kafka 生产者 .\bin\winodws\kafka-console-producer.bat --broker-list localhost:9092 --topic t1
    //kafka 消费者 .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic t1 --from-beginning


    // 连接此socket获取输入数据
    val text = env.socketTextStream("localhost", 8686, '\n')
    val topic = "t1"
    val prop = new Properties
    prop.setProperty("bootstrap.servers", "localhost:9092")
    prop.setProperty("transaction.timeout.ms",60000*15+"")
   val myProducer =  new FlinkKafkaProducer[String](topic, new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),prop,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    text.addSink(myProducer)
    env.execute(StreamKafkaSinkScalaScala.getClass.getName)


  }

}
