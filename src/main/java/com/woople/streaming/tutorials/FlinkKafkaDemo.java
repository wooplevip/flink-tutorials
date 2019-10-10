package com.woople.streaming.tutorials;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSink;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSource;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSinkBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSourceBase;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

public class FlinkKafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final TableSchema tableSchema = new TableSchema(new String[]{"imsi", "lac", "cell"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING});
        final TypeInformation<Row> typeInfo = tableSchema.toRowType();

        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        KafkaTableSourceBase kafkaTableSource = new Kafka010TableSource(
                tableSchema,
                "foo",
                properties,
                deserSchemaBuilder.build());


        tableEnv.registerTableSource("KafkaCsvTable", kafkaTableSource);

        Table kafkaCsvTable = tableEnv.scan("KafkaCsvTable");
        Table result = kafkaCsvTable.where("lac != '5'").select("imsi,lac,cell");

        DataStream ds = tableEnv.toAppendStream(result, typeInfo);

        final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(typeInfo).setFieldDelimiter('|').setQuoteCharacter('\0').setLineDelimiter("\r");

        KafkaTableSinkBase sink = new Kafka010TableSink(
                result.getSchema(),
                "bar",
                properties,
                Optional.of(new FlinkFixedPartitioner<>()),
                serSchemaBuilder.build());

        sink.emitDataStream(ds);

        env.execute("Flink kafka demo");
    }
}
