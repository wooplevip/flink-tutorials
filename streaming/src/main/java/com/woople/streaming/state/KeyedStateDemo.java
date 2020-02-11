package com.woople.streaming.state;

import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //env.setStateBackend(new FsStateBackend("file:///Users/peng/tmp/checkpoint", true));
        env.setParallelism(1);//
        //env.enableCheckpointing(1000);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(10);

        KeyedStream keyedStream = env.addSource(new DataSource()).keyBy(0);
        System.out.println(keyedStream.getParallelism() + "&&&&&&&&");

        keyedStream.flatMap(new CountWindowAverage())
                .print();

        env.execute("Keyed State demo");
    }

    private static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            int bound = 50;
            Long[] keys = new Long[]{1L, 2L, 3L};

            final long numElements = RandomUtils.nextLong(10, 20);
            int i = 0;
            while (running && i < numElements) {
                Thread.sleep(RandomUtils.nextLong(1, 5) * 1000L);
                Tuple2 data = new Tuple2<>(keys[RandomUtils.nextInt(0, 3)], RandomUtils.nextLong(1, 5));
                ctx.collect(data);
                System.out.println(Thread.currentThread().getId() + "---------------------------------sand data:" + data);
                i++;
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
