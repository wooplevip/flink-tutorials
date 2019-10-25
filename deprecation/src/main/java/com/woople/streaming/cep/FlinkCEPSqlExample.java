package com.woople.streaming.cep;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import java.util.Properties;

public class FlinkCEPSqlExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        final TableSchema tableSchema = new TableSchema(new String[]{"symbol","tax","price", "rowtime"}, new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.SQL_TIMESTAMP});
        final TypeInformation<Row> typeInfo = tableSchema.toRowType();
        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        FlinkKafkaConsumer010<Row> myConsumer = new FlinkKafkaConsumer010<>(
                "foo",
                deserSchemaBuilder.build(),
                properties);

        myConsumer.setStartFromLatest();

        DataStream<Row> stream = env.addSource(myConsumer).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        tableEnv.registerDataStream("Ticker", stream, "symbol,tax,price,rowtime.rowtime");

        Table result = tableEnv.sqlQuery("SELECT * " +
                "FROM Ticker " +
                "    MATCH_RECOGNIZE( " +
                "        PARTITION BY symbol " +
                "        ORDER BY rowtime " +
                "        MEASURES " +
                "            A.price AS firstPrice, " +
                "            B.price AS lastPrice " +
                "        ONE ROW PER MATCH " +
                "        AFTER MATCH SKIP PAST LAST ROW " +
                "        PATTERN (A+ B) " +
                "        DEFINE " +
                "            A AS A.price < 10, " +
                "            B AS B.price > 100 " +
                "    )");

        final TableSchema tableSchemaResult = new TableSchema(new String[]{"symbol","firstPrice","lastPrice"}, new TypeInformation[]{Types.STRING, Types.LONG, Types.LONG});
        final TypeInformation<Row> typeInfoResult = tableSchemaResult.toRowType();
        DataStream ds = tableEnv.toAppendStream(result, typeInfoResult);
        ds.print();
        env.execute("Flink CEP via SQL example");
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Row> {
        private final long maxOutOfOrderness = 5000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Row row, long previousElementTimestamp) {
            System.out.println("Row is " + row);
            long timestamp = StringUtilsPlus.dateToStamp(String.valueOf(row.getField(3)));
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("watermark:" + StringUtilsPlus.stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }
}
