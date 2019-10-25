package com.woople.streaming.tutorials;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkContextUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class FlinkKafkaDemoB {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final TableSchema tableSchema = new TableSchema(new String[]{"imsi","lac","cell"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING});
        final TypeInformation<Row> typeInfo = tableSchema.toRowType();
        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        FlinkKafkaConsumer010<Row> myConsumer = new FlinkKafkaConsumer010(
                "foo",
                deserSchemaBuilder.build(),
                properties);

        myConsumer.setStartFromLatest();

        DataStream<Row> stream = env.addSource(myConsumer);
        tableEnv.registerDataStream("KafkaCsvTable", stream);
        Table kafkaCsvTable = tableEnv.scan("KafkaCsvTable");
        Table result = kafkaCsvTable.where("lac != '0'").select("imsi,lac,cell");

        final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(typeInfo).setFieldDelimiter(',').setLineDelimiter("\r");
        FlinkKafkaProducer010<Row> myProducer = new FlinkKafkaProducer010<>(
                "host-10-1-236-139:6667",
                "bar",
                serSchemaBuilder.build());

        Properties properties1 = new Properties();
        properties1.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        DataStream ds = tableEnv.toAppendStream(result, typeInfo);

        properties1.setProperty("topicxx", "bar");

        ds.timeWindowAll(Time.of(5, TimeUnit.SECONDS))
                .apply(new PartialModelBuilder())
                .writeUsingOutputFormat(new KafkaOutputFormat(properties1));


//
//        myProducer.setFlushOnCheckpoint(true);






        env.execute("Flink kafka demo");
    }

    private static class KafkaOutputFormat implements OutputFormat<List<Row>> {
        private KafkaProducer<String, String> producer;
        private Properties producerConfig;
        //private SPKeyedSerializationSchema schema;
        private String targetTopic;

        public KafkaOutputFormat(Properties producerConfig) {
            this.producerConfig = producerConfig;

            targetTopic = producerConfig.getProperty("topicxx");

            producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            producerConfig.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        }

        @Override
        public void configure(Configuration configuration) {

            System.out.println(configuration + "23444444");

        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            producer = getKafkaProducer(this.producerConfig);
        }

        @Override
        public void writeRecord(List<Row> rows) throws IOException {
            for (Row row : rows) {
                System.out.println(row + "=xyz");

                ProducerRecord<String, String> record = new ProducerRecord<>(targetTopic, String.valueOf(row.getField(0)), row.toString());

                producer.send(record);
            }
        }

        @Override
        public void close() throws IOException {

            producer.close();

        }

        private  <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
            return new KafkaProducer<>(props);
        }
    }


    public static class PartialModelBuilder implements AllWindowFunction<Row, List<Row>, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(TimeWindow timeWindow, Iterable<Row> values, Collector<List<Row>> out) throws Exception {
            ArrayList<Row> rows = Lists.newArrayList(values);

            if (rows.size() > 0){
                System.out.println("5 seconds collect " + rows.size());

                out.collect(rows);
            }
        }
    }
}


