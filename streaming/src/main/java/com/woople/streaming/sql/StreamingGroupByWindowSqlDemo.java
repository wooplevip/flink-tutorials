package com.woople.streaming.sql;

import com.woople.streaming.utils.MemoryAppendStreamTableSink;
import com.woople.streaming.utils.StringUtilsPlus;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

public class StreamingGroupByWindowSqlDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        DataStream<Tuple4<Long, String, Integer, Long>> ds = env.addSource(new DataSource()).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());
        tableEnv.registerDataStream("Orders", ds, "user, product, amount, rowtime.rowtime");

        // compute SUM(amount) per day (in event-time)
        Table result1 = tableEnv.sqlQuery(
                "SELECT user, " +
                        "  TUMBLE_START(rowtime, INTERVAL '5' SECOND) as wStart,  " +
                        "  SUM(amount) " +
                        "  FROM Orders " +
                        "GROUP BY TUMBLE(rowtime, INTERVAL '5' SECOND), user");

        String[] fieldNames = {"user", "wStart", "amounts"};
        TypeInformation[] fieldTypes = {Types.LONG, Types.SQL_TIMESTAMP, Types.INT};

        TableSink sink = new MemoryAppendStreamTableSink(fieldNames, fieldTypes);
        tableEnv.registerTableSink("output", sink);
        result1.insertInto("output");

        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple4<Long, String, Integer, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
            String[] products = new String[]{"iPhoneX", "iPhone11", "iPhone11 Pro Max"};

            final long numElements = 10;
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 3) * 1000L);

                long random = RandomUtils.nextInt(3, 15);
                long rowtime = 0;

                if (random%2==0){
                    rowtime = System.currentTimeMillis() - random*1000L;
                }else {
                    rowtime = System.currentTimeMillis() + random*1000L;
                }


                Tuple4 data = new Tuple4<Long, String, Integer, Long>(RandomUtils.nextLong(1, 2),
                        products[RandomUtils.nextInt(0, 3)],
                        RandomUtils.nextInt(1, 5)*1000,
                        rowtime);
                ctx.collect(data);
                System.out.println("sand data:" + data + " rowtime=" + StringUtilsPlus.stampToDate(rowtime));
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Long>> {

        private final long maxOutOfOrderness = 10000;

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple4<Long, String, Integer, Long> value, long previousElementTimestamp) {
            //System.out.println("value is " + value);
            long timestamp = value.f3;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            //System.out.println("watermark:" + StringUtilsPlus.stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }
}
