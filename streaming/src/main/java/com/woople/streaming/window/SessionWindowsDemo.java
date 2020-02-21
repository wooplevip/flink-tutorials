package com.woople.streaming.window;

import com.google.common.collect.Lists;
import com.woople.streaming.utils.StringUtilsPlus;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

public class SessionWindowsDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        KeyedStream<Tuple3<String, Integer, Integer>, Tuple> input = env.addSource(new DataSource()).keyBy(0);
        input.window(ProcessingTimeSessionWindows.withGap(Time.seconds(4)))
                .apply(new WindowFunction<Tuple3<String, Integer, Integer>, List<Tuple3<String, Integer, Integer>>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Integer, Integer>> input, Collector<List<Tuple3<String, Integer, Integer>>> out) throws Exception {
                        System.out.println("Window1=" + StringUtilsPlus.stampToDate(window.getStart()) + "-" + StringUtilsPlus.stampToDate(window.getEnd()));
                        List<Tuple3<String, Integer, Integer>> list = Lists.newArrayList(input);
                        out.collect(list);
                    }
                }).print("Result1:");

        input.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public long extract(Tuple3<String, Integer, Integer> element) {
                        return element.f1 * 1000L;
                    }
                }))
                .apply(new WindowFunction<Tuple3<String, Integer, Integer>, List<Tuple3<String, Integer, Integer>>, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple3<String, Integer, Integer>> input, Collector<List<Tuple3<String, Integer, Integer>>> out) throws Exception {
                System.out.println("Window2=" + StringUtilsPlus.stampToDate(window.getStart()) + "-" + StringUtilsPlus.stampToDate(window.getEnd()));
                List<Tuple3<String, Integer, Integer>> list = Lists.newArrayList(input);
                out.collect(list);
            }
        }).print("Result2:");

        env.execute();
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, Integer, Integer>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Integer>> ctx) throws Exception {
            final long numElements = 30;
            int i = 0;
            while (running && i < numElements) {
                long r = RandomUtils.nextLong(1, 8);
                Thread.sleep(r * 1000L);
                Tuple3 data = new Tuple3<>("foo", RandomUtils.nextInt(1, 9), i);
                ctx.collect(data);
                System.out.println("Sand data:" + data + " at " + StringUtilsPlus.stampToDate(System.currentTimeMillis()));
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
