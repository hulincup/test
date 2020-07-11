package com.suning.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import org.apache.flink.api.java.DataSet;


/**
 * @author lynn
 */
public class WordCountJava {
    public static void main(String[] args) throws Exception {
        //获取环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件 创建dataSource
        String path = "C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt";
        DataSource<String> stringDataSource = executionEnvironment.readTextFile(path);
        //DataSet<String> dataSet = executionEnvironment.fromElements("hello world hello java hello flink");
        //操作
        AggregateOperator<Tuple2<String, Integer>> wordCount = stringDataSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                .groupBy(0)
                .sum(1);


        //打印输出
        wordCount.print();

    }
}
