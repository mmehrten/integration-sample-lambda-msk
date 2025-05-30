package com.amazonaws.kafka.samples;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class ProcessRecords {

    private static final Logger logger = LogManager.getLogger(ProcessRecords.class);
    
    private long getKafkaEventRecordsSize(KafkaEvent kafkaEvent) {
        long size = 0L;
        for (Map.Entry<String, List<KafkaEvent.KafkaEventRecord>> kafkaEventEntry : kafkaEvent.getRecords().entrySet()) {
            size += kafkaEventEntry.getValue().size();
        }
        return size;
    }

    private void addToFirehoseBatch(KafkaEvent kafkaEvent, String requestId, SendKinesisDataFirehose sendKinesisDataFirehose) {
        kafkaEvent.getRecords().forEach((key, value) -> value.forEach(v -> {
            sendKinesisDataFirehose.addFirehoseRecordToBatch(v.getValue().toString().concat("\n"), requestId);
        }));
    }

    void processRecords(KafkaEvent kafkaEvent, String requestId) {
        logger.info("Processing batch with {} records for Request ID {} \n", getKafkaEventRecordsSize(kafkaEvent), requestId);
        SendKinesisDataFirehose sendKinesisDataFirehose = new SendKinesisDataFirehose();
        addToFirehoseBatch(kafkaEvent, requestId, sendKinesisDataFirehose);
        SendKinesisDataFirehose.sendFirehoseBatch(sendKinesisDataFirehose.getFirehoseBatch(), 0, requestId, SendKinesisDataFirehose.batchNumber.incrementAndGet());
        SendKinesisDataFirehose.batchNumber.set(0);
    }
}
