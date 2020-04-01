package com.woople.streaming.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        //Time-bounded stream joins are only supported in event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Tuple3[] clickData = new Tuple3[]{
                new Tuple3<>("Mary", "./home", 1584034230000L),//2020-03-13 1:30:30
                new Tuple3<>("Liz", "./opt", 1584034238000L),//2020-03-13 1:30:38
                new Tuple3<>("Bob", "./cart", 1584034240000L)//2020-03-13 1:30:40
        };

        Tuple3[] locationData = new Tuple3[]{
                new Tuple3<>("Mary", "A", 1584034228000L),//2020-03-13 1:30:28
                new Tuple3<>("Liz", "C", 1584034242000L),//2020-03-13 1:30:42
                new Tuple3<>("Bob", "B", 1584034244000L)//2020-03-13 1:30:44
        };

        DataStream<Tuple3<String, String, Long>> orangeStream = env.addSource(new JoinDataSource("orangeStream", clickData)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });
        DataStream<Tuple3<String, String, Long>> greenStream = env.addSource(new JoinDataSource("greenStream", locationData)).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });

        orangeStream
                .keyBy(0)
                .intervalJoin(greenStream.keyBy(0))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Object>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context ctx, Collector<Object> out) throws Exception {
                        out.collect(new Tuple5<>(left.f0, left.f1, left.f2, right.f1, right.f2));
                    }
                }).name("intervalJoin").print("Interval Join Demo : ");

        env.execute("Interval Join Demo");
    }
}
