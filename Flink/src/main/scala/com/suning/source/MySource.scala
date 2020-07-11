package com.suning.source

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random


object MySource {
  //创建流执行环境
  val environment = StreamExecutionEnvironment.getExecutionEnvironment

  //6.自定义数据
  val stream5 = environment.addSource(new mySensorSource())


  stream5.print("stream")

  //流环境一定要执行,类比spark
  environment.execute("SourceTest")
}




//自定义生成测试数据源的SourceFunction
case class mySensorSource() extends RichSourceFunction[SensorReading] {
  // 定义一个标识位，用来表示数据源是否正常运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }
  // 随机生成10个传感器的温度数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 创建一个随机数生成器
    val rand = new Random()

    // 初始化10个传感器的温度值，随机生成，包装成二元组（id, temperature）
    var curtemp = 1.to(10).map(
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )

    // 无限循环生成数据，如果cancel的话就停止
    while(running){
      // 更新当前温度值，再之前温度上增加微小扰动
      curtemp = curtemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前时间戳，包装样例类
      val curTs = System.currentTimeMillis()
      curtemp.foreach(
        data => ctx.collect( SensorReading( data._1, curTs, data._2 ) )
      )
      // 间隔200ms
      Thread.sleep(200)
    }
  }

}

