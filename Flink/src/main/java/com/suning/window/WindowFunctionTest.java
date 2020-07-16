package com.suning.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @author lynn
 */
public class WindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = map
                .keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5));

        //input   output    type of key   window that the window function can be applied
        SingleOutputStreamOperator<String> sum = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {

                int sum = 0;
                for (Tuple2<String, Integer> tuple2 : input) {
                    sum += tuple2.f1;
                }
                long start = window.getStart();
                long end = window.getEnd();
                out.collect("key:" + tuple.getField(0) + ",sum:" + sum + ",start:" + start + ",end:" + end);

            }
        });

        sum.print();

        env.execute("WindowFunctionTest");
    }
}
