package com.woople.streaming.join;

import com.woople.streaming.utils.StringUtilsPlus;
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
        DataStream<Tuple2<String, Integer>> orangeStream = env.addSource(new DataSource1());

        DataStream<Tuple2<String, Integer>> greenStream = env.addSource(new DataSource2());


        DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(orangeStream, greenStream, 4);
        joinedStream.print("join");
        env.execute("Windowed Join Demo");
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {

        return grades.join(salaries)
                .where(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> v) throws Exception {
                        return v.f0;
                    }
                })
                .equalTo(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> v) throws Exception {
                        return v.f0;
                    }
                })
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

    private static class DataSource1 extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Tuple2[] datas = new Tuple2[]{
                    new Tuple2<>("foo", 1),
                    new Tuple2<>("bar", 3),
                    new Tuple2<>("baz", 2),
                    new Tuple2<>("foo", 5)};

            final long numElements = datas.length;
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 3) * 1000L);
                ctx.collect(datas[i]);
                System.out.println("Sand data1:" + datas[i] + " at " + StringUtilsPlus.stampToDate(System.currentTimeMillis()));
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class DataSource2 extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Tuple2[] datas = new Tuple2[]{
                    new Tuple2<>("foo", 11),
                    new Tuple2<>("bar", 33),
                    new Tuple2<>("baz", 22),
                    new Tuple2<>("xyz", 55)
            };

            final long numElements = datas.length;
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                ctx.collect(datas[i]);
                System.out.println("Sand data2:" + datas[i]+ " at " + StringUtilsPlus.stampToDate(System.currentTimeMillis()));
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
