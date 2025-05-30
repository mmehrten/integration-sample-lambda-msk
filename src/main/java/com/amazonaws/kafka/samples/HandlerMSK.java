package com.amazonaws.kafka.samples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.lambda.powertools.kafka.Deserialization;
import software.amazon.lambda.powertools.kafka.DeserializationType;
import software.amazon.lambda.powertools.logging.Logging;

public class HandlerMSK implements RequestHandler<ConsumerRecords<String, String>, String> {
    
    private static final Logger logger = LoggerFactory.getLogger(HandlerMSK.class);
    
    private void processRecords(ConsumerRecords<String, String> records, String requestId) {
        SendKinesisDataFirehose sendKinesisDataFirehose = new SendKinesisDataFirehose();
        for (ConsumerRecord<String, String> record : records) {
            sendKinesisDataFirehose.addFirehoseRecordToBatch(record.value().concat("\n"), requestId);
        }
        SendKinesisDataFirehose.sendFirehoseBatch(sendKinesisDataFirehose.getFirehoseBatch(), 0, requestId, SendKinesisDataFirehose.batchNumber.incrementAndGet());
        SendKinesisDataFirehose.batchNumber.set(0);
    }

    @Override
    @Logging(logEvent = true)
    @Deserialization(type = DeserializationType.KAFKA_JSON)
    public String handleRequest(ConsumerRecords<String, String> records, Context context) {
        logger.info("Processing batch with {} records for Request ID {} \n", records.count(), context.getAwsRequestId());
        processRecords(records, context.getAwsRequestId());
        return "200 OK";
    }

}
