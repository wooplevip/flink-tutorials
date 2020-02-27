package com.woople.streaming.task;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

public class TaskDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        env.addSource(new DataSource())
                .map(new MyMapFunction())
                .keyBy(0)
                .process(new MyKeyedProcessFunction())
                .addSink(new DataSink()).setParallelism(1).name("Custom Sink");

        System.out.println(env.getExecutionPlan());
        env.execute();
    }

    private static class MyMapFunction implements MapFunction<Tuple2<Long, String>, Tuple2<Long, String>> {
        @Override
        public Tuple2<Long, String> map(Tuple2<Long, String> value) throws Exception {
            System.out.println(Thread.currentThread().getName() + " - key: " + value.f0);
            return new Tuple2<>(value.f0, value.f1 + "-map");
        }
    }

    private static class MyKeyedProcessFunction extends KeyedProcessFunction<Tuple, Tuple2<Long, String>, Tuple2<Long, String>> {
        @Override
        public void processElement(Tuple2<Long, String> value, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
            System.out.println(Thread.currentThread().getName() + " - key: " + ctx.getCurrentKey());
            out.collect(value);
        }
    }

    private static class DataSink implements SinkFunction<Tuple2<Long, String>> {
        @Override
        public void invoke(Tuple2<Long, String> value, Context context) throws Exception {
            System.out.println("Result:" + value);
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
                Thread.sleep(RandomUtils.nextLong(1, 10) * 1000L);

                Tuple2<Long, String> data = new Tuple2<>(RandomUtils.nextLong(0, 10),
                        products[RandomUtils.nextInt(0, products.length)]);
                ctx.collect(data);
                System.out.println("Sand data:" + data);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
