package com.suning.key;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

public class KeyTest3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataSream = environment.socketTextStream("bd1301", 7777);

        KeyedStream<String, String> keyedDS = dataSream.keyBy(new KeySelector<String, String>() {
            public String getKey(String s) throws Exception {
                return s.split(" ")[1];
            }
        });
        SingleOutputStreamOperator<String> reduce = keyedDS.reduce(new ReduceFunction<String>() {
            public String reduce(String s1, String s2) throws Exception {
                return s1 + s2;
            }
        });
        reduce.print();


        //执行
        environment.execute("KeyTest3");
    }
}
