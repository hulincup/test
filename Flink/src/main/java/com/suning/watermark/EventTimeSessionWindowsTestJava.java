package com.suning.watermark;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;


/**
 * @author lynn
 */
public class EventTimeSessionWindowsTestJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置自动生成wms时间 默认200ms,processingTime
        env.getConfig()
                .setAutoWatermarkInterval(1000);

        DataStreamSource<String> inputStream = env.socketTextStream("bd1301", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> dataStream = inputStream
                .map(new MapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(String value) throws Exception {
                        String[] str = value.split(" ");
                        return new Tuple2<String, Long>(str[0], Long.parseLong(str[1]));
                    }
                })

                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, Long>>() {
                    //设置延迟时间1s
                    Long bound = 1000L;
                    //过来数据的最大时间戳
                    Long maxTs = 0L;
                    //Long maxTs = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(maxTs - bound);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
                        System.out.println("========="+element.f1);
                        maxTs = Math.max(maxTs, element.f1 * 1000L);
                        System.out.println("========="+maxTs);
                        return element.f1 * 1000L;
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);
        dataStream.print();

        env.execute();


    }
}
