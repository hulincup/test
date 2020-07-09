package com.suning

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val environment = ExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度
    environment.setParallelism(1)

    //读取文件
    //val path = "C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt"
    val path="./hello.txt"
    val dataSet: DataSet[String] = environment.readTextFile(path)

    //进行flatmap
    val wordCountDataSet = dataSet.flatMap(_.split("\\s")).map((_,1)).groupBy(0).sum(1)

    //对结果打印输出
    wordCountDataSet.print()
  }

}
