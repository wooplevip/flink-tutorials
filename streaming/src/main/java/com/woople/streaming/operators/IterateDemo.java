package com.woople.streaming.operators;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class IterateDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> orangeStream = env.addSource(new DataSource());

        IterativeStream<Tuple2<String, Integer>> iteration = orangeStream.iterate(5000);
        DataStream<Tuple2<String, Integer>> iterationBody = iteration.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                return new Tuple2<>(value.f0, value.f1-5);
            }
        });

        DataStream<Tuple2<String, Integer>> feedback = iterationBody.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return value.f1 > 25;
            }
        });

        iteration.closeWith(feedback);

        DataStream<Tuple2<String, Integer>> output = iterationBody.filter(new FilterFunction<Tuple2<String, Integer>>() {
            @Override
            public boolean filter(Tuple2<String, Integer> value) throws Exception {
                return value.f1 <= 25;
            }
        });

        feedback.print("Iterate feedback");
        output.print("Iterate output");
        env.execute("Iterate Demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            int bound = 20;
            String[] keys = new String[]{"foo", "bar", "baz"};

            final long numElements = RandomUtils.nextLong(10, 20);
            int i = 0;

            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                if (i == 0){
                    ctx.collect(new Tuple2<>(keys[RandomUtils.nextInt(0, 3)], 36));
                }else {
                    Tuple2 data = new Tuple2<>(keys[RandomUtils.nextInt(0, 3)], RandomUtils.nextInt(10, bound));
                    ctx.collect(data);
                    System.out.println(Thread.currentThread().getId() + "-sand data:" + data);
                }
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
