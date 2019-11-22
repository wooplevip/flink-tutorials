package com.woople.streaming.base;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaSerializationSchemaImpl implements KafkaSerializationSchema<KafkaEvent> {
    private String topic = "bar";

    public KafkaSerializationSchemaImpl(String topic) {
        this.topic = topic;
    }

    public KafkaSerializationSchemaImpl() {
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(KafkaEvent event, @Nullable Long timestamp) {
        return new ProducerRecord<>(this.topic, event.toString().getBytes());
    }
}
