package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class);

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
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "Hello, Kafka!");

        // ProducerRecord 전송
        try {
            RecordMetadata recordMetadata = producer.send(producerRecord).get();
            logger.info("\n ####### record metadata received #######");
            logger.info("partition:" + recordMetadata.partition());
            logger.info("offset:" + recordMetadata.offset());
            logger.info("timestamp:" + recordMetadata.timestamp());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
