package com.woople.streaming.operators;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> input = env.fromElements(new Tuple2<>("foo", 1), new Tuple2<>("foo", 2),
                new Tuple2<>("bar", 3), new Tuple2<>("baz", 4),
                new Tuple2<>("bar", 4), new Tuple2<>("baz", 5));

        input.print("Input data");
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = input.keyBy(0);
        keyed.print("Keyed data");
        env.execute("KeyBy Demo");
    }
}
