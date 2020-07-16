package com.suning.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author lynn
 */
public class StreamWordCountJava {
    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.disableOperatorChaining();
        //获取流
        DataStreamSource<String> dataStream = environment.socketTextStream("bd1301", 7777);

        //对流进行操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamWordCount = dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
                .keyBy(0)
         //       .timeWindow(Time.seconds(5))
                .sum(1);

        //打印输出
        streamWordCount.print();

        //执行任务(流计算一定要记得开启流)
        environment.execute("StreamWordCountJava");

    }
}
