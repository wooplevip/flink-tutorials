package com.woople.streaming.tutorials;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;

public class FlinkKafkaProcessingTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        final TableSchema tableSchema = new TableSchema(new String[]{"imsi","lac","cell"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING});

        final TypeInformation<Row> typeInfo = tableSchema.toRowType();
        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        FlinkKafkaConsumer010<Row> myConsumer = new FlinkKafkaConsumer010<>(
                "foo",
                deserSchemaBuilder.build(),
                properties);

        myConsumer.setStartFromLatest();

        DataStream<Row> stream = env.addSource(myConsumer);

        Table result = tableEnv.fromDataStream(stream, "imsi,lac,cell,processingTime2.proctime") ;

        Table at = result.window(Slide.over("10.seconds").every("10.seconds").on("processingTime2").as("PTWindow"))
                .groupBy("lac,cell, PTWindow")
                .select("lac,cell,PTWindow.end as windowEndTime, imsi.count as cnt");


        final TableSchema outputSchema = new TableSchema(new String[]{"lac", "cell", "windowEndTime", "cnt"}, new TypeInformation[]{Types.STRING,Types.STRING, Types.SQL_TIMESTAMP, Types.LONG});
        final TypeInformation<Row> outputTypeInfo = outputSchema.toRowType();


        DataStream ds = tableEnv.toAppendStream(at, outputTypeInfo);

        final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(outputTypeInfo).setFieldDelimiter(',').setLineDelimiter("\r");

        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");
        FlinkKafkaProducer010<Row> myProducer = new FlinkKafkaProducer010<>(
                "bar",
                serSchemaBuilder.build(),
                producerProperties);

        myProducer.setWriteTimestampToKafka(true);
        ds.addSink(myProducer);

        env.execute("Flink kafka demo");
    }
}
