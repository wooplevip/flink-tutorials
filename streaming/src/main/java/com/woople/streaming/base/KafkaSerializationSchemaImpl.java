package com.woople.streaming.base;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaSerializationSchemaImpl implements KafkaSerializationSchema<KafkaEvent> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaEvent event, @Nullable Long timestamp) {
        return new ProducerRecord<>("bar", event.toString().getBytes());
    }
}
