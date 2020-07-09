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

public class MyUdfTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("MyUdfTest")
                .master("local[2]")
                .getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

        JavaRDD<String> rdd = javaSparkContext.parallelize(Arrays.asList("zhangsan", "lisi", "wamgwu"));

        //得到rowRDD
        JavaRDD<Row> rowRdd = rdd.map(new Function<String, Row>() {
            @Override
            public Row call(String value) throws Exception {
                return RowFactory.create(value);
            }
        });

        //得到schema
        List<StructField> list = Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(list);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(rowRdd, schema);
        dataFrame.printSchema();
        dataFrame.show();
        dataFrame.createOrReplaceTempView("user");
        //注册udf函数  注意有返回值类型
        MyUdf myUdf = new MyUdf();
        sparkSession.udf().register("strLen",myUdf,DataTypes.IntegerType);

        //使用udf函数
        Dataset<Row> result = sparkSession.sql("select name,strLen(name) from user");

        result.show();

        sparkSession.stop();


    }
}
