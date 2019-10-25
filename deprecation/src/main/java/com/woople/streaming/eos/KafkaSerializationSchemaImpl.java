package com.woople.streaming.eos;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class KafkaSerializationSchemaImpl implements KafkaSerializationSchema<String> {
    @Override
    public ProducerRecord<byte[], byte[]> serialize(String s, @Nullable Long aLong) {
        return null;
    }
}
