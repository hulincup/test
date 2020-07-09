package com.suning.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class MyHiveUdfNew extends UDF {
    public int evaluate(int value) {
        return value + 10;
    }
}
