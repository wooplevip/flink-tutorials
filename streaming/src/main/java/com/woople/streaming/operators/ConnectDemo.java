package com.woople.streaming.operators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

public class ConnectDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> orangeStream = env.addSource(new DataSource1());
        DataStream<Tuple3<String, Integer, Integer>> greenStream = env.addSource(new DataSource2());

        orangeStream.connect(greenStream).flatMap(new CoFlatMapFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Object>() {
            @Override
            public void flatMap1(Tuple2<String, Integer> value, Collector<Object> out) throws Exception {
                if (!value.f0.contains("@")){
                    out.collect(new Tuple3<>(value.f0, value.f1, RandomUtils.nextInt(0, value.f1)));
                }
            }

            @Override
            public void flatMap2(Tuple3<String, Integer, Integer> value, Collector<Object> out) throws Exception {
                for (String s : value.f0.split("@")) {
                    out.collect(new Tuple3<>(s, value.f1, value.f2));
                }
            }
        }).print("Connect");

        env.execute("Connect Demo");
    }

    private static class DataSource1 extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            int bound = 50;
            String[] keys = new String[]{"foo@xxyz", "bar", "baz"};

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

    private static class DataSource2 extends RichParallelSourceFunction<Tuple3<String, Integer, Integer>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, Integer, Integer>> ctx) throws Exception {
            int bound = 50;
            String[] keys = new String[]{"foo@xxyz", "bar", "baz"};

            final long numElements = RandomUtils.nextLong(10, 20);
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                Tuple3 data = new Tuple3<>(keys[RandomUtils.nextInt(0, 3)], RandomUtils.nextInt(0, bound), RandomUtils.nextInt(0, bound));
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
