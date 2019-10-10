package com.woople.streaming.tutorials;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;

public class SPKeyedSerializationSchema implements KeyedSerializationSchema<Row> {
    private SerializationSchema<Row> serializationSchema;

    public SPKeyedSerializationSchema(SerializationSchema<Row> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }


    @Override
    public byte[] serializeKey(Row row) {
        Row r = new Row(1);
        r.setField(0, row.getField(0));

        return serializationSchema.serialize(r);
    }

    @Override
    public byte[] serializeValue(Row row) {
        return serializationSchema.serialize(row);
    }

    @Override
    public String getTargetTopic(Row row) {
        return null;
    }
}
