package com.suning.spark.sql.udfAndUdaf;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class MyUdafTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("MyUdfTest")
                .master("local[2]")
                .getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        JavaRDD<Long> rdd = javaSparkContext.parallelize(Arrays.asList(20L, 10L, 30L),2);

        //得到rowRDD
        JavaRDD<Row> rowRdd = rdd.map(new Function<Long, Row>() {
            @Override
            public Row call(Long value) throws Exception {
                return RowFactory.create(value);
            }
        });

        //得到schema
        List<StructField> structFields = Arrays.asList(DataTypes.createStructField("age", DataTypes.LongType, true));
        StructType schema = DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRdd, schema);
        dataFrame.printSchema();
        dataFrame.show();
        dataFrame.createOrReplaceTempView("user");
        //注册udf函数  注意有返回值类型
        MyUdaf myUdaf = new MyUdaf();
        //注册udaf函数没有返回值类型 DataTypes.DoubleType 因为在定义的时候已经在dataType()函数说明了返回值类型
        sparkSession.udf().register("average",myUdaf);

        //使用自定义udaf函数average
        Dataset<Row> result = sparkSession.sql("select average(age) as average from user");
        //使用sql自带的聚合函数avg
        //Dataset<Row> result = sparkSession.sql("select avg(age) as average from user");

        result.show();

        sparkSession.stop();

    }
}
