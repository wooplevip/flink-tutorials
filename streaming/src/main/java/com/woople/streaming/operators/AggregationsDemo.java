package com.woople.streaming.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationsDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, Integer, Long>> input = env.fromElements(
                new Tuple3<>("foo", 8, 1573441871000L), new Tuple3<>("foo", 2, 1573441872000L),
                new Tuple3<>("foo", 9, 1573441873000L), new Tuple3<>("foo", 4, 1573441874000L),
                new Tuple3<>("foo", 17, 1573441875000L), new Tuple3<>("foo", 5, 1573441876000L),
                new Tuple3<>("foo", 17, 1573441877000L));

        KeyedStream<Tuple3<String, Integer, Long>, Tuple> keyed = input.keyBy(0);

        keyed.sum(1).print("sum");
        keyed.min(1).print("min");
        keyed.minBy(1).print("minBy");
        keyed.max(1).print("max");
        keyed.maxBy(1, true).print("maxBy");
        keyed.maxBy(1, false).print("maxBy");

        env.execute("Aggregations Demo");
    }
}
