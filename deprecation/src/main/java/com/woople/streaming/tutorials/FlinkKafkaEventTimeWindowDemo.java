package com.woople.streaming.tutorials;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.GroupWindowedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;

import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 测试数据
 *
 * 1,1559292600000 2019-05-31 16:50:00
 * 9,1559292610000 2019-05-31 16:50:10
 *
 * 2,1559292670000 2019-05-31 16:51:10
 *
 * 4,1559292650000 2019-05-31 16:50:50
 * 3,1559292620000 2019-05-31 16:50:20
 * 90,1559292651000 2019-05-31 16:50:51
 *
 * 3,1559293200000 2019-05-31 17:00:00
 * 4,1559292960000 2019-05-31 16:56:00
 * 5,1559292000000 2019-05-31 16:40:00
 *
 *
 *
 *
 * */

public class FlinkKafkaEventTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        final TableSchema tableSchema = new TableSchema(new String[]{"id","eventTime"}, new TypeInformation[]{Types.LONG,Types.LONG});

        final TypeInformation<Row> typeInfo = tableSchema.toRowType();
        final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInfo).setFieldDelimiter(',');

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");

        FlinkKafkaConsumer010<Row> myConsumer = new FlinkKafkaConsumer010<>(
                "xyzzy",
                deserSchemaBuilder.build(),
                properties);

        myConsumer.setStartFromLatest();

        DataStream<Row> _stream = env.addSource(myConsumer);

        DataStream<Row> stream = _stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        Table result = tableEnv.fromDataStream(stream, "id, eventTime.rowtime") ;

        Table at = result.window(Tumble.over("60.seconds").on("eventTime").as("ETWindow"))
                .groupBy("ETWindow").select("id.sum as s, ETWindow.start as start, ETWindow.end as end");

        final TableSchema outputSchema = new TableSchema(new String[]{"s", "start", "end"}, new TypeInformation[]{Types.LONG, Types.SQL_TIMESTAMP,Types.SQL_TIMESTAMP});
        final TypeInformation<Row> outputTypeInfo = outputSchema.toRowType();
        DataStream ds = tableEnv.toAppendStream(at, outputTypeInfo);


        ds.print();

        env.execute("Flink kafka demo");
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Row> {

        private final long maxOutOfOrderness = 20000;

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Row row, long previousElementTimestamp) {
            System.out.println("====****" + row);
            long timestamp = Long.valueOf(String.valueOf(row.getField(1)));
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("watermark:" + stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            // return the watermark as current highest timestamp minus the out-of-orderness bound

            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        private static String stampToDate(String s){
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }
}
