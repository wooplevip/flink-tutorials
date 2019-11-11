package com.woople.streaming.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Exercise01 {
    /**
     *  输入为以逗号为分隔符的字符串，例如 "1, 2, 3, 4, 5"
     *  把每个整型元素加一，然后值大于3的输出
     */

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Integer> myInts = env.fromElements("1, 2, 3, 4, 5")
                .flatMap(new FlatMapFunction<String, Integer>() {
                    @Override
                    public void flatMap(String value, Collector<Integer> out) throws Exception {
                        for (String word : value.split(",")) {
                            out.collect(Integer.parseInt(word.trim()));
                        }
                    }
                }).map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) throws Exception {
                        return value + 1;
                    }
                }).filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value > 3;
                    }
                });

        myInts.print();
        env.execute("Exercise01");
    }
}
