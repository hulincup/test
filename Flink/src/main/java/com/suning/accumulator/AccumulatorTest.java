package com.suning.accumulator;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import redis.clients.jedis.Tuple;

public class AccumulatorTest {
    public static void main(String[] args) throws Exception {
        //批处理
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //读取文件 创建dataSource
        String path = "C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt";
        DataSource<String> dataSource = env.readTextFile(path);
        MapOperator<String, String> map = dataSource.map(new RichMapFunction<String, String>() {
            IntCounter intCounter = new IntCounter();

            //继承了AbstractRichFunction  重写open方法
            @Override
            public void open(Configuration parameters) throws Exception {
                //getRuntimeContext()这个方法是父类的
                getRuntimeContext().addAccumulator("myAcc", intCounter);

            }

            //实现了MapFunction接口 重写map方法
            @Override
            public String map(String value) throws Exception {

                if ("shanghai".contains(value)) {
                    intCounter.add(1);
                }
                //String[] strArr = value.split(" ");
                //System.out.println(strArr.length);
                //return new Tuple2<String, String>(strArr[0], strArr[1]);
                return value;
            }
        });

        map.writeAsText("C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\accumulator.txt", FileSystem.WriteMode.NO_OVERWRITE);

       // map.writeAsCsv("C:\\\\Java\\\\IdeaProjects\\\\Bigdata_Review\\\\Flink\\\\src\\\\main\\\\resources\\\\accumulator.csv");
        JobExecutionResult execute = env.execute();
        Integer myAcc = execute.getAccumulatorResult("myAcc");
        System.out.println(myAcc);


    }
}
