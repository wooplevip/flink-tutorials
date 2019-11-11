package com.woople.streaming.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Exercise02 {
    /**
     *  输入为以逗号为分隔符的字符串，例如 "hello,2,1573441871000L"，
     *  第一列表示一个单词，第二列表示单词出现的次数，第三列表示这条记录产生的时间
     *
     *  并输出出现次数最多的单词并把这条数据的时间戳转换为日期格式，如果次数一样多输出最后一条
     */

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, Integer, Long>> input = env.fromElements(
                new Tuple3<>("foo", 12, 1573441871000L), new Tuple3<>("foo", 2, 1573441872000L),
                new Tuple3<>("bar", 9, 1573441873000L), new Tuple3<>("bar", 4, 1573441874000L),
                new Tuple3<>("foo", 12, 1573441875000L), new Tuple3<>("baz", 5, 1573441876000L),
                new Tuple3<>("baz", 17, 1573441877000L));

        KeyedStream<Tuple3<String, Integer, Long>, Tuple> keyed = input.keyBy(0);

        keyed.maxBy(1, false).map(new MapFunction<Tuple3<String, Integer, Long>, Object>() {
            @Override
            public Object map(Tuple3<String, Integer, Long> value) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                return new Tuple3(value.f0, value.f1, simpleDateFormat.format(new Date(value.f2)));
            }
        }).print();

        env.execute("Exercise02");
    }
}
