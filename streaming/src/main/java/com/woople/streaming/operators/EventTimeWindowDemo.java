package com.woople.streaming.operators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class EventTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<String, Integer, Long>> orangeStream = env.addSource(new DataSource("orangeStream"))
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        orangeStream.keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                .apply(new WindowFunction<Tuple3<String, Integer, Long>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<Object> out) throws Exception {
                        System.out.println(window.toString());
                        out.collect(input);
                    }
                }).name("EventTimeWindow").print("out");

        env.execute("EventTime Demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, Integer, Long>> {

        private volatile boolean running = true;
        private volatile String name;

        public DataSource(String name) {
            this.name = name;
        }

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
            Random random = new Random();
            int bound = 100;
            final long numElements = 10;
            int i = 0;

            while (running && i < numElements) {
                Thread.sleep(1500);
                Tuple3 data = new Tuple3<>("foo", random.nextInt(bound), getRandomInt(i*10, 60+i*10));
                ctx.collect(data);
                System.out.println(Thread.currentThread().getId() + "-" + this.name + "-sand data:" + data);
                i++;
            }
            Thread.sleep(5000);
        }

        @Override
        public void cancel() {
            running = false;
        }

        private long getRandomInt(int min, int max){
            return 1573441860000L + 1000* RandomUtils.nextInt(min, max);
        }
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Long>> {
        private final long maxOutOfOrderness = 10000;
        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(Tuple3<String, Integer, Long> row, long previousElementTimestamp) {
            long timestamp = row.f2;
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println(Thread.currentThread().getId() + "-" + row + ",time="+stampToDate(row.f2.toString()) + ",watermark=" + stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        private static String stampToDate(String s) {
            String res;
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            long lt = new Long(s);
            Date date = new Date(lt);
            res = simpleDateFormat.format(date);
            return res;
        }
    }
}
