package com.example.kafka;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {

    // kafka-topics --bootstrap-server localhost:9092 --create --topic pizza-topic --partitions 3

    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        String topicName = "pizza-topic";

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // 피자 메시지 전송
        sendPizzaMessage(kafkaProducer, topicName, -1, 100, 100, 1000, true);
        kafkaProducer.close();
    }

    public static void sendPizzaMessage(
            KafkaProducer<String, String> kafkaProducer,
            String topicName,
            int iterCount,
            int interIntervalMillis,
            int intervalMillis,
            int intervalCount,
            boolean isSync) {

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = new Faker(new Locale("ko"), random);

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));

            sendPizzaMessage(kafkaProducer, producerRecord, pMessage, isSync);

            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("########## IntervalCount : " + intervalCount);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("@@@@@@@@@@@ interIntervalMillis : " + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        ProducerRecord<String, String> producerRecord,
                                        HashMap<String, String> pMessage,
                                        boolean isSync) {
        if (!isSync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message" + pMessage.get("key") + "partition:" + metadata.partition() + ", offset:" + metadata.offset());
                } else {
                    logger.error("Error while producing message to topic : " + exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message" + pMessage.get("key") + "partition:" + recordMetadata.partition());
                logger.info("offset:" + recordMetadata.offset());
                logger.info("timestamp:" + recordMetadata.timestamp());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
