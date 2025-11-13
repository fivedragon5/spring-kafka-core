package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerMTpoicRebalance {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerMTpoicRebalance.class);

    public static void main(String[] args) {
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps());
        kafkaConsumer.subscribe(List.of("topic-p3-t1", "topic-p3-t2"));

        // mainThread 참조
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread를 통해 kafkaconsumer의 wakeup() 호출
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("============ shutdown hook ============");
            kafkaConsumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                logger.error("Interrupted Exception", e);
            }
        }));

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("topic: {} | offset : {} | key : {} | partition : {} | value : {}",
                            record.topic(), record.offset(), record.key(), record.partition(), record.value());
                }
            }
        } catch (WakeupException e) {
            logger.error("Wakeup Exception", e);
        } finally {
            kafkaConsumer.close();
            logger.info("============ finally Kafka Consumer closed ============");
        }
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-assign");
        props.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        return props;
    }
}
