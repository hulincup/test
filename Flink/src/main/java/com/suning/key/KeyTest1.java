package com.suning.key;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSream = environment.socketTextStream("bd1301", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapDS = dataSream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                return new Tuple2<String, Integer>(line.split(" ")[0], 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = mapDS.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            /**
             * reduce操作的两个参数:
             * @param t1  表示的是已经进过聚合的Tuple2
             * @param t2  需要进行聚合的新的Tuple2
             * @return
             * @throws Exception
             */
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<String, Integer>(t2.f0 + t1.f0, t2.f1 + t1.f1);
            }
        });

        reduce.print();

        //开启
        environment.execute("KeyTest1");
    }
}
