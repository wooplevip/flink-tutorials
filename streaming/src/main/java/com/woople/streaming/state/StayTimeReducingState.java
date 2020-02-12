package com.woople.streaming.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StayTimeReducingState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //f0:手机号，f0:当前位置，f1:驻留时长
        DataStream<Tuple3<String, String, Long>> source = env.fromElements("19911111111,A,6", "19911111112,A,12", "19911111111,A,3"
                , "19911111112,A,6", "19911111111,A,12", "19911111112,A,9")
                .map(new MapFunction<String, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(String value) throws Exception {
                        String[] v = value.split(",");
                        return new Tuple3(v[0], v[1], Long.valueOf(v[2]));
                    }
                });
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = source.keyBy(0);

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            //累计驻留时长
            private ReducingState<Long> stayAreaTimeSum;
            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<Long> sumDescriptor =
                        new ReducingStateDescriptor<>(
                                "stayAreaTimeSum",
                                new ReduceFunction<Long>() {
                                    @Override
                                    public Long reduce(Long value1, Long value2) throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                TypeInformation.of(new TypeHint<Long>() {
                                }));
                stayAreaTimeSum = getRuntimeContext().getReducingState(sumDescriptor);
            }

            @Override
            public Tuple3<String, String, Long> map(Tuple3<String, String, Long> value) throws Exception {
                stayAreaTimeSum.add(value.f2);
                long timeSum = stayAreaTimeSum.get();
                return new Tuple3<>(value.f0, value.f1, timeSum);
            }
        }).print();

        env.execute("StayTimeReducingState demo");
    }
}
