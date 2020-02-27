package com.woople.streaming.window;

import com.google.common.collect.Lists;
import com.woople.streaming.utils.StringUtilsPlus;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class SessionWindowsEventTimeDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Integer, Long>, Tuple> input = env.addSource(new DataSource())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Long>>(Time.seconds(20)) {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Long> element) {
                        return element.f2;
                    }
                }).keyBy(0);
        input.window(EventTimeSessionWindows.withGap(Time.seconds(5)))
                .apply(new WindowFunction<Tuple3<String, Integer, Long>, List<Tuple3<String, Integer, Long>>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<List<Tuple3<String, Integer, Long>>> out) throws Exception {
                        System.out.println("Window1=" + StringUtilsPlus.stampToDate(window.getStart()) + "-" + StringUtilsPlus.stampToDate(window.getEnd()));
                        List<Tuple3<String, Integer, Long>> list = Lists.newArrayList(input);
                        out.collect(list);
                    }
                }).print("Result1:");

        input.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<String, Integer, Long>>() {
                    @Override
                    public long extract(Tuple3<String, Integer, Long> element) {
                        return element.f1 * 1000L;
                    }
                }))
                .apply(new WindowFunction<Tuple3<String, Integer, Long>, List<Tuple3<String, Integer, Long>>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<List<Tuple3<String, Integer, Long>>> out) throws Exception {
                System.out.println("Window2=" + StringUtilsPlus.stampToDate(window.getStart()) + "-" + StringUtilsPlus.stampToDate(window.getEnd()));
                List<Tuple3<String, Integer, Long>> list = Lists.newArrayList(input);
                out.collect(list);
            }
        }).print("Result2:");

        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, Integer, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Long>> ctx) throws Exception {
            Tuple3<String, Integer, Long>[] d = new Tuple3[]{
                    new Tuple3<>("foo", 1, 1582336800000L),
                    new Tuple3<>("foo", 2, 1582336801000L),
                    new Tuple3<>("foo", 3, 1582336804000L),
                    new Tuple3<>("foo", 4, 1582336805000L),
                    new Tuple3<>("foo", 5, 1582336811000L),
                    new Tuple3<>("foo", 6, 1582336815000L)
            };
            final long numElements = d.length;

            int i = 0;
            while (running && i < numElements) {
                long r = RandomUtils.nextLong(1, 5);
                Thread.sleep(r * 1000L);
                ctx.collect(d[i]);
                System.out.println("Sand data:" + d[i]);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
