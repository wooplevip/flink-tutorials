//package com.woople.streaming.eos;
//
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//
//import java.util.Properties;
//
//public class FlinkEOSDemo {
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "host-10-1-236-179:9093");
//        properties.setProperty("group.id", "group_eos");
//
//        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
//                "foo",
//                new SimpleStringSchema(),
//                properties);
//
//        DataStream<String> stream = env.addSource(myConsumer);
//
//        stream.map((MapFunction<String, String>) value -> "foo-"+value);
//
//
//        Properties myProducerProperties = new Properties();
//        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>("bar",
//                new KafkaSerializationSchemaImpl(),
//                myProducerProperties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
//
//
//        stream.addSink(myProducer);
//
//        env.execute("EOS Kafka Example");
//    }
//}
