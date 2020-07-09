package com.suning.spark.sql.udfAndUdaf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class MyUdaf extends UserDefinedAggregateFunction {
    /**
     * 函数输入的数据结构
     * @return
     */
    @Override
    public StructType inputSchema() {
        List<StructField> list = Arrays.asList(DataTypes.createStructField("age", DataTypes.LongType, true));
        return DataTypes.createStructType(list);
    }

    /**
     *计算时的数据结构
     * @return
     */
    @Override
    public StructType bufferSchema() {
        List<StructField> list = Arrays.asList(
                DataTypes.createStructField("sum", DataTypes.LongType, true),
                DataTypes.createStructField("count", DataTypes.LongType,true)
                );
        return DataTypes.createStructType(list) ;
    }

    /**
     *函数返回时的数据类型
     * @return
     */
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    /**
     * 函数是否稳定(相同输入是否有相同的输出)
     * @return
     */
    @Override
    public boolean deterministic() {
        return true;
    }

    /**
     * 计算之前的缓冲区初始化 buffer是一个数组
     * @param buffer
     */
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0,0L);
        buffer.update(1,0L);
    }

    /**
     * 根据查询结果更新缓冲区的数据
     * @param buffer
     * @param input
     */
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)) {
            long sum = buffer.getLong(0) + input.getLong(0);
            long count = buffer.getLong(1) + 1;
            buffer.update(0, sum);
            buffer.update(1, count);
        }
    }

    /**
     * 将多个节点的缓冲区合并
     * @param buffer1
     * @param buffer2
     */
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        long sum = buffer1.getLong(0) + buffer2.getLong(0);
        long count = buffer1.getLong(1) + buffer2.getLong(1);
        buffer1.update(0, sum);
        buffer1.update(1, count);
    }

    /**
     * 计算
     * @param buffer
     * @return
     */
    @Override
    public Double evaluate(Row buffer) {
        return ((double) buffer.getLong(0) / buffer.getLong(1));
    }
}
