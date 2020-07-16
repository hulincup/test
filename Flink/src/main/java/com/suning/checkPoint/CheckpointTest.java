package com.suning.checkPoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author lynn
 * @Date 2020/7/14 19:02
 */
public class CheckpointTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CheckpointConfig chkConfig = env.getCheckpointConfig();
        //设置语义,semantic
        chkConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //stateBackend类型

        env.setStateBackend(new FsStateBackend("hdfs://bd1301:8020/flink/checkpoint",false));
        //chk每隔多少毫秒执行一次
        chkConfig.setCheckpointInterval(5000);
        //超时时间,连接外部系统可能会连接不上
        chkConfig.setCheckpointTimeout(5000);
        //最大并行执行的chk为1个
        chkConfig.setMaxConcurrentCheckpoints(1);
        //上一个chk结束后500ms触发下一个chk
        chkConfig.setMinPauseBetweenCheckpoints(500);
        //重启策略,重试三次,每次间隔500ms
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,500));
        //失败前最大重试次数
        env.setRestartStrategy(RestartStrategies.failureRateRestart(1, Time.milliseconds(500),Time.milliseconds(100)));

        DataStreamSource<String> ds = env.socketTextStream("bd1301", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = ds
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words){
                        out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .sum(1);
        sum.print();

        env.execute("CheckpointTest");
    }
}
