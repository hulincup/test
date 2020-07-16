package com.suning.sql;

import com.suning.table.Person;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author lynn
 * @Date 2020/7/15 14:47
 */
public class DataSetSQL {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(env);

        DataSource<Person> dataSource = env.readCsvFile("C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\person.txt")
                .ignoreFirstLine()
                .pojoType(Person.class, "id", "name", "age", "score", "cls");

        batchTableEnv.createTemporaryView("person",dataSource);


        Table table = batchTableEnv.sqlQuery("select * from person where score > 90");
        table.printSchema();

        DataSet<Person> dataSet = batchTableEnv.toDataSet(table, Person.class);
        //batchTableEnv.toDataSet(table, Row.class);

        dataSet.print();



    }
}
