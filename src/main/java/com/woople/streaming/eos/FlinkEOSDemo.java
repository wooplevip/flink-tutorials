package com.woople.streaming.eos;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class FlinkEOSDemo {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-179:9093");
        properties.setProperty("group.id", "group_foo");

        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                "foo",
                new SimpleStringSchema(),
                properties);

        DataStream<String> stream = env.addSource(myConsumer);

        stream.map((MapFunction<String, String>) value -> "foo-"+value);

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "host-10-1-236-179:9093",            // broker list
                "bar",                  // target topic
                new SimpleStringSchema());   // serialization schema


        stream.addSink(myProducer);
    }
}
