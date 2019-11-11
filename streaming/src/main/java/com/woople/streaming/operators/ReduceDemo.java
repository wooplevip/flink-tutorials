package com.woople.streaming.operators;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> input = env.fromElements(new Tuple2<>("foo", 1), new Tuple2<>("foo", 2),
                new Tuple2<>("bar", 3), new Tuple2<>("baz", 4),
                new Tuple2<>("bar", 4), new Tuple2<>("baz", 5));

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = input.keyBy(0);

        DataStream<Tuple2<String, Integer>> out = keyed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        });

        out.print("reduce");
        env.execute("Reduce Demo");
    }
}
