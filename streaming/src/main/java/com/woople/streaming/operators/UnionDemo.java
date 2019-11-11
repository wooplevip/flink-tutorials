package com.woople.streaming.operators;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class UnionDemo {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple2<String, Integer>> orangeStream = env.addSource(new DataSource("orangeStream"));
        DataStream<Tuple2<String, Integer>> greenStream = env.addSource(new DataSource("greenStream"));

        orangeStream.union(greenStream).print("union");
        env.execute("Union Demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Integer>> {
        private volatile boolean running = true;
        private volatile String name;

        public DataSource(String name) {
            this.name = name;
        }

        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            Random random = new Random();
            int bound = 100;
            final long numElements = 6;
            int i = 0;

            while (running && i < numElements) {
                Thread.sleep(1500);
                Tuple2 data = new Tuple2<>("foo", random.nextInt(bound));
                ctx.collect(data);
                System.out.println(Thread.currentThread().getId() + "-" + this.name + "-sand data:" + data);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
