package com.suning.spark.sql.udfAndUdaf;

import org.apache.spark.sql.api.java.UDF1;
import scala.Tuple2;

public class MyUdf implements UDF1<String, Integer> {
    @Override
    public Integer call(String s) throws Exception {
        return s.length();
    }
}
