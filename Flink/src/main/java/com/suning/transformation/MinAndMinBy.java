package com.suning.transformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;
import java.util.List;

public class MinAndMinBy {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple3<String, String, Integer>> list = new ArrayList<Tuple3<String, String, Integer>>();
        list.add( new Tuple3<String,String,Integer>("zs","male",200));
        list.add( new Tuple3<String,String,Integer>("lisi","male",100));
        list.add( new Tuple3<String,String,Integer>("wangwu","male",50));
        list.add( new Tuple3<String,String,Integer>("zhaoliu","male",36));
        DataStreamSource<Tuple3<String, String, Integer>> ds = environment.fromCollection(list);

        //ds.keyBy(1).min(2).print();
        ds.keyBy(1).minBy(2).print();

        environment.execute();


    }
}
