package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncCustomCallback {

    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCallback.class);

    public static void main(String[] args) {
        // KafkaProducer configuration setting, Map을 사용해도 됨
        Properties props = new Properties();
        // bootstrap.servers
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key.serializer
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        // value.serializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // topic 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic simple-topic
        // kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic
        String topicName = "multipart-topic";

        // KafkaProducer 객체 생성
        KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {
            // ProducerRecord 객체 생성
            ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "Hello, Kafka ASync with Key!!-" + seq);

            Callback callback = new CustomCallback(seq);

            // ProducerRecord 전송
            kafkaProducer.send(producerRecord, callback);
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        kafkaProducer.close();
    }
}
