package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {

    private final Logger logger = LoggerFactory.getLogger(CustomCallback.class);
    private final int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            logger.info("seq:{} partitions:{} offset:{}", this.seq, metadata.partition(), metadata.offset());
        } else {
            System.err.println("Error while producing message: " + exception.getMessage());
        }
    }
}
