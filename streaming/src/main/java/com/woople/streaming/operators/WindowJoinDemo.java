package com.woople.streaming.operators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


public class WindowJoinDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        DataStream<Tuple2<String, Integer>> orangeStream = env.addSource(new DataSource());
        DataStream<Tuple2<String, Integer>> greenStream = env.addSource(new DataSource());
        DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(orangeStream, greenStream, 5);
        joinedStream.print("join");
        env.execute("Windowed Join Demo");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {

        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSize)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {

                    @Override
                    public Tuple3<String, Integer, Integer> join(
                            Tuple2<String, Integer> first,
                            Tuple2<String, Integer> second) {
                        return new Tuple3<String, Integer, Integer>(first.f0, first.f1, second.f1);
                    }
                });
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
