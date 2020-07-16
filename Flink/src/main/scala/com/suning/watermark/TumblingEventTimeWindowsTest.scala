package com.suning.watermark

import com.suning.source.SensorReading
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

//1001, 1547718199, 35.8
object TumblingEventTimeWindowsTest {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置timewindow的时间为eventTime  点进去可以看到默认的200ms
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //默认200ms
    env.getConfig.setAutoWatermarkInterval(100L)

    //读取数据
    val inputStream: DataStream[String] = env.socketTextStream("bd1301", 7777)


    //map成样例类型
    val dataStream: DataStream[(String, Double)] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }
    )
      //   .assignAscendingTimestamps(_.timeStamp * 1000L) //顺序的数据 不需要触发

      /*.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timeStamp * 1000L
      })*/

     // .assignTimestampsAndWatermarks(new MyAssigner2())
      .assignTimestampsAndWatermarks(new MyAssigner()) //乱序的数据进行处理  读进来的dataStream


      .map(data => (data.id, data.temperature)) //转换成只有两个属性的元组

      //  .keyBy(0)根据索引
      .keyBy(_._1) //根据元组的取值方式
      //开窗  size=10
      .timeWindow(Time.seconds(10))
      //取最小温度
      .reduce((data1, data2) => (data1._1, data1._2.min(data2._2)))

    dataStream.print()

    env.execute("TumblingEventTimeWindowsTest")


  }

}

//普通类的括号()
class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  //定义固定延迟1s  延迟多少秒发送  这里所有的时间都是EventTime 从数据中提取的extractTimestamp
  val bound: Long = 1 * 1000L
  //定义当前收到的最大时间戳
  var maxTs: Long = Long.MinValue

  //这个方法每100ms调用一次  注意{}
  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    //最大的时间戳
    maxTs = maxTs.max(element.timeStamp * 1000L)
    element.timeStamp * 1000L
  }
}


class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  val bound: Long = 1000L

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if (lastElement.id == "sensor_1")
      new Watermark(extractedTimestamp - bound)
    else
      null
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = element.timeStamp * 1000L
}