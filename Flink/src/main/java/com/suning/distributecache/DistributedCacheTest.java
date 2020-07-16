package com.suning.distributecache;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

/**
 * @author lynn
 */
public class DistributedCacheTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //将指定的目录文件读取过来放在缓存中(hdfs或者本地系统)
        env.registerCachedFile("C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt","cache");

        //直接读取
        DataSource<String> dataStream = env.readTextFile("C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt");

        //DataSource<String> dataSource = env.fromElements("hadoop", "hbase", "hive", "kafka", "spark", "flink");

        MapOperator<String, String> map = dataStream.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {

                File file = getRuntimeContext().getDistributedCache().getFile("cache");
                List<String> list = FileUtils.readLines(file);
                for (String line : list) {
                    System.out.println(line);
                }


            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });


        map.print();

    }
}
