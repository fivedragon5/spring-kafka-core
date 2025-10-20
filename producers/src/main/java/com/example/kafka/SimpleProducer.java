package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // KafkaProducer configuration setting, Map을 사용해도 됨
        Properties props = new Properties();
        // bootstrap.servers
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key.serializer
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value.serializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // topic 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
        // kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic
        String topicName = "simple-topic";

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // ProducerRecord 객체 생성
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "Hello, Kafka!");

        // ProducerRecord 전송
        producer.send(record);
        producer.flush();
        producer.close();

    }
}
