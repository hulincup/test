package com.suning.broadcast;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lynn
 */
public class Broadcasttest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt");

        ArrayList<String> list = new ArrayList<>();
        list.add("shanghai");
        list.add("beijing");
        DataSource<String> whiteDs = env.fromCollection(list);

        FilterOperator<String> f1 = dataSource.filter(new RichFilterFunction<String>() {
            List<String> whiteName = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                whiteName = getRuntimeContext()
                        .getBroadcastVariable("white-name");

            }

            @Override
            public boolean filter(String value) throws Exception {
                for (String white : whiteName){
                    if (value.contains(white)) {
                        return true;
                    }
                }
                return false;
            }
        });

        FilterOperator<String> f2 = f1.withBroadcastSet(whiteDs, "white-name");
        f2.print();

    }
}
