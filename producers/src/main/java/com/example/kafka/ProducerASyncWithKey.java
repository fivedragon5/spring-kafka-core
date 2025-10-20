package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncWithKey {

    public static final Logger logger = LoggerFactory.getLogger(ProducerASyncWithKey.class);

    public static void main(String[] args) {
        // KafkaProducer configuration setting, Map을 사용해도 됨
        Properties props = new Properties();
        // bootstrap.servers
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // key.serializer
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value.serializer
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // topic 생성 : kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic --partitions 3
        // kafka-console-consumer --bootstrap-server localhost:9092 --topic simple-topic
        String topicName = "multipart-topic";

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        for (int seq = 0; seq < 20; seq++) {
            // ProducerRecord 객체 생성
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "Hello, Kafka ASync with Key!!-" + seq);
            // ProducerRecord 전송
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("\n ####### record metadata received #######");
                    logger.info("key:" + producerRecord.key());
                    logger.info("partition:" + metadata.partition());
                    logger.info("offset:" + metadata.offset());
                    logger.info("timestamp:" + metadata.timestamp());
                } else {
                    logger.error("Error while producing message to topic : " + exception.getMessage());
                }
            });
        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        kafkaProducer.close();
    }
}
