package com.suning.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author lynn
 */
public class KafkaSinkJava {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        String path = "C:\\Java\\IdeaProjects\\Bigdata_Review\\Flink\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> ds = environment.readTextFile(path);

        ds.addSink(new FlinkKafkaProducer011<String>("bd1301:9092","kafkasink",new SimpleStringSchema()));

        //开启流环境
        environment.execute("KafkaSink");

    }
}
