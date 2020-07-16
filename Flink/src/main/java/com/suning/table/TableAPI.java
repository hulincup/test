package com.suning.table;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @Author lynn
 * @Date 2020/7/15 14:12
 */
public class TableAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //流处理sql环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

        ExecutionEnvironment batchEenv = ExecutionEnvironment.getExecutionEnvironment();
        //批处理sql环境
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEenv);

        //读取csv文件
        DataSource<Person> dataSource = batchEenv.readCsvFile("C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\person.txt")
                .ignoreFirstLine()
                .pojoType(Person.class, "id", "name", "age", "score", "cls");

        //将dataSource注册成临时表
        //batchTableEnv.registerDataSet("person",dataSource); 过时了
        batchTableEnv.createTemporaryView("person",dataSource);
        //batchTableEnv.scan("person"); 过时了
        Table person = batchTableEnv.from("person");

        //打印表的元数据信息
        person.printSchema();

        Table result = person.select("id,name,age,score,cls").where("score>90");
        result.printSchema();

        DataSet<Person> dataSet = batchTableEnv.toDataSet(result, Person.class);
        dataSet.print();



    }
}
