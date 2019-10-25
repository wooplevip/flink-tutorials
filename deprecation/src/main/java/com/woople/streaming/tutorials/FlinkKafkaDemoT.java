package com.woople.streaming.tutorials;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;

public class FlinkKafkaDemoT {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final TableSchema tableSchema = new TableSchema(new String[]{"imsi","lac","cell"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING});
        final TypeInformation<Row> typeInfo = tableSchema.toRowType();
        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        FlinkKafkaConsumer010<Row> myConsumer = new FlinkKafkaConsumer010<>(
                "foo",
                deserSchemaBuilder.build(),
                properties);

        myConsumer.setStartFromLatest();

        DataStream<Row> stream = env.addSource(myConsumer);
        tableEnv.registerDataStream("KafkaCsvTable", stream);
        Table kafkaCsvTable = tableEnv.scan("KafkaCsvTable");
        Table result = kafkaCsvTable.where("lac != '5'").select("imsi,lac,cell");

        final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(typeInfo).setFieldDelimiter(',').setLineDelimiter("\r");

        DataStream ds = tableEnv.toAppendStream(result, typeInfo);

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");
        producerProperties.setProperty("linger.ms", "5000");
        FlinkKafkaProducer010<Row> myProducer = new FlinkKafkaProducer010<>(
                "bar",
                serSchemaBuilder.build(),
                producerProperties);

        myProducer.setWriteTimestampToKafka(true);
        ds.addSink(myProducer);

        System.out.println("send to producer 1");

        Properties producerProperties1 = new Properties();
        producerProperties1.setProperty("bootstrap.servers", "host-10-1-236-139:6667");
        producerProperties1.setProperty("linger.ms", "10000");
        FlinkKafkaProducer010<Row> myProducer1 = new FlinkKafkaProducer010<>(
                "baz",
                serSchemaBuilder.build(),
                producerProperties1);

        myProducer1.setWriteTimestampToKafka(true);

        ds.addSink(myProducer1);

        System.out.println("send to producer 2");

        env.execute("Flink kafka demo");
    }
}
