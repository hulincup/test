package com.suning.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 整个模式匹配的规则在5秒内，如果连续两次登录失败，则发出警告。。
 */
public class LoginFailWarningDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStream<LoginEvent> loginEventStream = env.fromCollection(Arrays.asList(
                new LoginEvent("1", "192.168.0.1", "fail"),
                new LoginEvent("1", "192.168.0.2", "fail"),
                new LoginEvent("1", "192.168.0.3", "fail"),
                new LoginEvent("2", "192.168.10.10", "fail"),
                new LoginEvent("2", "192.168.10.10", "success")

        ));
        //开启一个模式匹配规则
        Pattern<LoginEvent, LoginEvent> begin = Pattern.begin("begin");
        //模式匹配的条件
        Pattern<LoginEvent, LoginEvent> p1 = begin.where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> ctx) throws Exception {
                return loginEvent.getType().equals("fail");
            }
        });
        //追加一个新的模式。匹配事件必须直接跟着先前的匹配事件（严格连续性）
        Pattern<LoginEvent, LoginEvent> next = p1.next("next");
        //新模式的匹配条件
        Pattern<LoginEvent, LoginEvent> p2 = next.where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> ctx) throws Exception {
                return loginEvent.getType().equals("fail");
            }
        });
        //定义事件序列进行模式匹配的最大时间间隔。 如果未完成的事件序列超过此时间，则将其丢弃：
        Pattern<LoginEvent, LoginEvent> p3 = p2.within(Time.seconds(5));

        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEventStream.keyBy("userId"), p3);


        SingleOutputStreamOperator<LoginWarning> loginFailDataStream = patternStream.select(new PatternSelectFunction<LoginEvent, LoginWarning>() {
            @Override
            public LoginWarning select(Map<String, List<LoginEvent>> pattern) throws Exception {
                List<LoginEvent> begin1 = pattern.get("begin");
                System.out.println("======== begin list ======");

                for (LoginEvent loginEvent : begin1) {
                    System.out.println(loginEvent);
                }

                System.out.println("======== next list ======");
//
                List<LoginEvent> next1 = pattern.get("next");
//
                for (LoginEvent loginEvent : next1) {
                    System.out.println(loginEvent);
                }

                return new LoginWarning(next1.get(0).getUserId(), next1.get(0).getType(), next1.get(0).getIp());
            }
        });


        loginFailDataStream.printToErr();

        env.execute();
    }
}
