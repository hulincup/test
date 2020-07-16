package com.suning.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


import java.text.SimpleDateFormat;

/**
 * @author lynn
 */
public class TrumblingWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> ds = env.socketTextStream("bd1301", 7777);


        SingleOutputStreamOperator<Tuple2<String, Integer>> map = ds.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
                long timeMillis = System.currentTimeMillis();
                System.out.println("value:" + value + ",time:" + simpleDateFormat.format(timeMillis));
                return new Tuple2<>(value, 1);
            }
        });


            map
                .keyBy(0)
               .timeWindow(Time.seconds(5))
     //               .countWindow(10)
                .sum(1)
                .print();

        env.execute("TrumblingWindowTest");
    }
}
