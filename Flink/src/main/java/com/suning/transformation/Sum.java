package com.suning.transformation;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;



import java.util.Random;

public class Sum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        String[] str = new String[]{"HELLO SHANGHAI","HELLO BEIJING","HELLO WUHAN"};
        DataStreamSource<String> ds = environment.fromElements(str);


        SingleOutputStreamOperator<Tuple2<String, Integer>> flatmapDS = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split(" ")) {

                    int num = new Random().nextInt(100);
                    System.out.println(word + num);
                    Thread.sleep(1000);
                    out.collect(new Tuple2(word, num));

                }
            }
        });

        flatmapDS
                .keyBy(0)
                .sum(1)
                .print();

        environment.execute();
    }
}
