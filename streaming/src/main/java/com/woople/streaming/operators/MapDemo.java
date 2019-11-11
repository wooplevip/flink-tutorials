package com.woople.streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Integer> myInts = env.fromElements(1, 2, 3, 4, 5)
                .map(x -> x + 1);

        myInts.print("map");
        env.execute("Map Demo");
    }
}
