package com.suning;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author lynn
 */
public class Consumer {
    /**
     * KafkaConsumer：需要创建一个消费者对象，用来消费数据
     * ConsumerConfig：获取所需的一系列配置参数
     * ConsuemrRecord：每条数据都要封装成一个ConsumerRecord对象
     * @param args
     */
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers","bd1301:9092");
        properties.put("group.id","test");
        properties.put("enable.auto.commit",false);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //定义消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        //订阅topic
        kafkaConsumer.subscribe(Arrays.asList("kafka"));
        //死循环,一直等待数据来消费
        while (true){
            //拿到records
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            //对records进行遍历拿到每一个record
            for (ConsumerRecord<String,String> record : records){
                long offset = record.offset();
                String key = record.key();
                String value = record.value();
                System.out.println("offset="+offset+",key="+key+",value="+value);
            }
            //手动提交偏移量,异步的方式提交
            kafkaConsumer.commitSync();
        }
    }
}
