package com.woople.streaming.operators;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> input = env.addSource(new DataSource());

        input.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(6)))
                .trigger(new Trigger<Tuple2<String, Integer>, TimeWindow>() {
                    /**
                     * CONTINUE: do nothing,
                     * FIRE: trigger the computation,
                     * PURGE: clear the elements in the window, and
                     * FIRE_AND_PURGE: trigger the computation and clear the elements in the window afterwards.
                     */
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })
                .apply(new WindowFunction<Tuple2<String, Integer>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> values, Collector<Object> out) throws Exception {
                        int sum = 0;
                        String key = "";
                        for (Tuple2<String, Integer> t : values) {
                            sum += t.f1;
                            key = t.f0;
                        }
                        out.collect(new Tuple2<>(key, sum));
                    }
                }).name("windowedStream").print("Window");

        env.execute("Window Demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Tuple2[] data = new Tuple2[]{
                    new Tuple2<>("foo", 1), new Tuple2<>("bar", 2),
                    new Tuple2<>("bar", 3), new Tuple2<>("baz", 4),
                    new Tuple2<>("baz", 5), new Tuple2<>("foo", 6),
                    new Tuple2<>("foo", 7), new Tuple2<>("baz", 8),
                    new Tuple2<>("foo", 9), new Tuple2<>("baz", 10),
                    new Tuple2<>("foo", 11), new Tuple2<>("baz", 12)};
            final long numElements = data.length;
            int i = 0;
            Thread.sleep(2000);
            while (running && i < numElements) {
                Thread.sleep(1000);
                ctx.collect(data[i]);
                System.out.println("sand data:" + data[i]);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
