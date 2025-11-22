package com.example.kafka;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerCommit {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerCommit.class);

    public static void main(String[] args) {
        String topicName = "pizza-topic";

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps());
        kafkaConsumer.subscribe(List.of(topicName));

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

        pollCommitSync(kafkaConsumer);
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("####### loopCount: {} consumerRecord count:{}, loopCount++", loopCount++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("offset : {} | key : {} | partition : {} | value : {}",
                            record.offset(), record.key(), record.partition(), record.value());
                }

                try {
                    logger.info("main thread is sleeping for 10 seconds...", loopCount * 10000);
                    Thread.sleep(10000); // 10초간 처리 지연 시뮬레이션
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        } catch (WakeupException e) {
            logger.error("Wakeup Exception", e);
        } finally {
            kafkaConsumer.close();
            logger.info("============ finally Kafka Consumer closed ============");
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> kafkaConsumer) {
        int loopCount = 0;

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
                logger.info("####### loopCount: {} consumerRecord count:{}, loopCount++", loopCount++, consumerRecords.count());

                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("offset : {} | key : {} | partition : {} | value : {}",
                            record.offset(), record.key(), record.partition(), record.value());
                }
                try {
                    if (consumerRecords.count() > 0) {
                        kafkaConsumer.commitSync();
                        logger.info("commit sync has been called.");
                    }
                } catch (CommitFailedException e) {
                    logger.error("Commit Failed Exception", e);
                }
            }
        } catch (WakeupException e) {
            logger.error("Wakeup Exception", e);
        } catch (Exception e) {
            logger.error("Exception", e);
        }
        finally {
            kafkaConsumer.close();
            logger.info("============ finally Kafka Consumer closed ============");
        }
    }

    private static Properties consumerProps() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_03");
//        props.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}
