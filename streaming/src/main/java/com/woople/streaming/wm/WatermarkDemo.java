package com.woople.streaming.wm;

import com.woople.streaming.utils.MemoryAppendStreamTableSink;
import com.woople.streaming.utils.StringUtilsPlus;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

public class WatermarkDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.setParallelism(1);

        DataStream<Tuple2<Long, String>> ds = env.addSource(new DataSource());
        ds.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
        .keyBy(1)
        .timeWindow(Time.seconds(10))
        .reduce(new ReduceFunction<Tuple2<Long, String>>() {
            @Override
            public Tuple2<Long, String> reduce(Tuple2<Long, String> value1, Tuple2<Long, String> value2) throws Exception {
                return new Tuple2<>(value1.f0+value2.f0, value1.f1 + "-" + value2.f1);
            }
        }).print();

        env.execute();
    }

    private static class MyMapFunction implements MapFunction<Tuple2<Long, String>, Tuple2<Long, String>>{

        @Override
        public Tuple2<Long, String> map(Tuple2<Long, String> value) throws Exception {
            return value;
        }
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, String>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
            String[] products = new String[]{"a", "b", "c", "d", "e", "f", "g"};

            final long numElements = 10;
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 3) * 1000L);

                Tuple2 data = new Tuple2<Long, String>(1578996000000L,//System.currentTimeMillis() + RandomUtils.nextLong(1, 10) * 1000,
                        products[RandomUtils.nextInt(0, 3)]);
                ctx.collect(data);
                System.out.println("sand data:" + data);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple2<Long, String>> {

        private final long maxOutOfOrderness = 5000;

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple2<Long, String> value, long previousElementTimestamp) {
            System.out.println("value is " + value);
            long timestamp = value.f0;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("watermark:" + StringUtilsPlus.stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            System.out.println(Thread.currentThread().getId() + "=======" + (currentMaxTimestamp - maxOutOfOrderness));
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }

}
