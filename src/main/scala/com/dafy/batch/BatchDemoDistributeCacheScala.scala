package com.dafy.batch


import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

object BatchDemoDistributeCacheScala {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import  org.apache.flink.api.scala._

    //1 注册一个文件,使用本地文件代替
    env.registerCachedFile("c:\\data\\count\\test.txt","testdata")

    val data = env.fromElements("a","c","b","d")

    val result = data.map(new RichMapFunction[String,String] {
      val dataList = ListBuffer[String]()
      val strBuf:StringBuffer = null
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
//        使用文件
        val myfile = getRuntimeContext.getDistributedCache.getFile("testdata")
        val lines = FileUtils.readLines(myfile)
        val it = lines.iterator()
        while (it.hasNext){
          val line = it.next()
          println("line-->"+ line)
          dataList.append(line)
        }
      }

      override def map(value: String): String = {
        for(it<-dataList){
          this.strBuf.append(it)
        }
        
        value

      }
    })

    result.print()

  }
}
