package com.woople.streaming.operators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowCoGroupDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> orangeStream = env.addSource(new DataSource());
        DataStream<Tuple2<String, Integer>> greenStream = env.addSource(new DataSource());

        DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowCoGroup(orangeStream, greenStream, 10);

        joinedStream.print();

        env.execute("Windowed CoGroup Demo");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowCoGroup(
            DataStream<Tuple2<String, Integer>> orangeStream,
            DataStream<Tuple2<String, Integer>> greenStream,
            long windowSize) {

        return orangeStream.coGroup(greenStream)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSize)))
                .apply(new Join());
    }

    private static class Join implements CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>{

        @Override
        public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            first.forEach(x -> {
                second.forEach(y -> {
                    if (x.f0.equals(y.f0)){
                        out.collect(new Tuple3<>(x.f0, x.f1, y.f1));
                    }
                });
            });
        }
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            int bound = 50;
            String[] keys = new String[]{"foo", "bar", "baz"};

            final long numElements = RandomUtils.nextLong(10, 20);
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                Tuple2 data = new Tuple2<>(keys[RandomUtils.nextInt(0, 3)], RandomUtils.nextInt(0, bound));
                ctx.collect(data);
                System.out.println(Thread.currentThread().getId() + "-sand data:" + data);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
