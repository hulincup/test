package com.suning.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt"
    val ds: DataStream[String] = environment.readTextFile(path)

    // map成样例类类型
    ds.addSink(new FlinkKafkaProducer011[String]("bd1301:9092","sinktest",new SimpleStringSchema()))

    //开启流环境
    environment.execute("KafkaSink")
  }
}
