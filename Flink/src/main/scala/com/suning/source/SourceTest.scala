package com.suning.source

import java.util.Properties


import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

//定义样例类
case class SensorReading(id: String, timeStamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建流执行环境
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(2)

    //1.从集合中读取数据
    val stream1: DataStream[SensorReading] = environment.fromCollection(List(
      SensorReading("1001", 1547718199, 35.8),
      SensorReading("1001", 1547718199, 35.8),
      SensorReading("1001", 1547718199, 35.8)
    ))

    //2.element可以是任意类型的数据
    val stream2: DataStream[Any] = environment.fromElements(1,2,3,"hello","flink")

    //3.从文件读取
    //val stream3: DataStream[String] = environment.readTextFile("./sensor.txt")

    //4.从kafka读取文件 作为kafka消费者
    val properties = new Properties()
    properties.put("bootstrap.servers","bd1301:9092,bd1302:9092,bd1303:9092")
    properties.put("group.id","consumer_group")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset","latest")


    //5.从kafka读取数据
    val stream4: DataStream[String] = environment
      .addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))



    stream4.print("stream")

    //流环境一定要执行,类比spark
    environment.execute("SourceTest")

  }

}

