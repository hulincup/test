package com.suning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author lynn
 */
public class Producer {
    /**
     * KafkaProducer：需要创建一个生产者对象，用来发送数据
     * ProducerConfig：获取所需的一系列配置参数
     * ProducerRecord：每条数据都要封装成一个ProducerRecord对象
     * @param args
     */
    public static void main(String[] args) {
        Properties properties = new Properties();
        //集群broker-list
        properties.put("bootstrap.servers","bd1301:9092");
        properties.put("acks","all");
        properties.put("retries",1);
        //批次大小
        properties.put("batch.size", 16384);
        //等待时间
        properties.put("linger.ms", 1);
        //RecordAccumulator缓冲区大小  32M
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //拿到生产的record
        ProducerRecord<String, String> record = new ProducerRecord<>("kafka", "1001", "hello");
        kafkaProducer.send(record);
        //关闭
        kafkaProducer.close();
    }
}
