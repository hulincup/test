package com.suning.sql;

import com.suning.table.Person;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.crypto.MacSpi;

/**
 * @Author lynn
 * @Date 2020/7/15 14:58
 */
public class DataStreamSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        //读取数据socket
        DataStreamSource<String> dataStream = env.socketTextStream("bd1301", 7777);

        SingleOutputStreamOperator<Person> map = dataStream.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String line) throws Exception {
                String[] strArr = line.split(",");
                return new Person(strArr[0], strArr[1], Integer.parseInt(strArr[2]), Integer.parseInt(strArr[3]), strArr[4]);

            }
        });

        //创建临时视图
        streamTableEnv.createTemporaryView("person", map);

        //查询
        Table table = streamTableEnv.sqlQuery("select * from person where score > 90");

        table.printSchema();

        //转换成dataSet
        //DataStream<Tuple2<Boolean, Person>> ds = streamTableEnv.toRetractStream(table, Person.class);
        DataStream<Person> ds = streamTableEnv.toAppendStream(table, Person.class);
        //streamTableEnv.toRetractStream(table, Row.class);

        ds.print();

        //开启流环境
        env.execute("DataStreamSQL");


    }
}
