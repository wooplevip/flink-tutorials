package com.woople.streaming.tutorials;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class FlinkKafkaSQLDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final TableSchema tableSchema = new TableSchema(new String[]{"imsi","lac","cell"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING});
        final TypeInformation<Row> typeInfo = tableSchema.toRowType();
        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        FlinkKafkaConsumer010<Row> myConsumer = new FlinkKafkaConsumer010<>(
                "xyzzy",
                deserSchemaBuilder.build(),
                properties);

        myConsumer.setStartFromLatest();

        DataStream<Row> stream = env.addSource(myConsumer);
        tableEnv.registerDataStream("KafkaCsvTable", stream);

        Table result = tableEnv.sqlQuery("SELECT imsi, lac, cell FROM KafkaCsvTable WHERE imsi LIKE '%abc%'");

        DataStream ds = tableEnv.toAppendStream(result, typeInfo);

        ds.print();


        env.execute("Flink kafka demo");
    }
}
