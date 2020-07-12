package com.suning.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 *
 */
public class KafkaSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //连接kafka的配置文件
        Properties properties = new Properties();
        properties.put("bootstrap.servers","bd1301:9092,bd1302:9092,bd1303:9092");
        properties.put("group.id","consumer_group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset","latest");
        DataStreamSource<String> dataStreamSource = environment.addSource(new FlinkKafkaConsumer011<String>("sensor", new SimpleStringSchema(), properties));
        dataStreamSource.print();

        //开启
        try {
            environment.execute("KafkaSource");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
