package com.suning.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //创建流环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置全局并行度
    environment.setParallelism(1)
    //disable 任务链  将任务打散(将原来合并的算子拆开成一个一个)
    environment.disableOperatorChaining()
    //获取host和port
    val tool = ParameterTool.fromArgs(args)
    val host = tool.get("host")
    val port = tool.getInt("port")

    //获取流     当前算子不能合并    还有一个startNewChain
    val dataStream: DataStream[String] = environment.socketTextStream(host,port)//.disableChaining()

    //操作
    val wordCountDataStream: DataStream[(String, Int)] = dataStream
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    //打印显示
    wordCountDataStream.print().setParallelism(1)

    //开启流
    environment.execute("StreamingWordCount")




  }

}
