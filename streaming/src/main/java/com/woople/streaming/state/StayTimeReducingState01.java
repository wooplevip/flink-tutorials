package com.woople.streaming.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StayTimeReducingState01 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = env.addSource(new StateDataSource()).keyBy(0);

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            private ReducingState<Tuple2<String, Long>> stayAreaTimeReducing;// area, first entry time

            @Override
            public void open(Configuration parameters) throws Exception {
                ReducingStateDescriptor<Tuple2<String, Long>> descriptor =
                        new ReducingStateDescriptor<>(
                                "stayAreaTime",
                                new ReduceFunction<Tuple2<String, Long>>() {
                                    @Override
                                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                                        long switchAreaTimes = value1.f1;
                                        if (!value1.f0.equals(value2.f0)){
                                            switchAreaTimes = switchAreaTimes + 1;
                                        }
                                        return new Tuple2<>(value2.f0, switchAreaTimes);
                                    }
                                },
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));
                stayAreaTimeReducing = getRuntimeContext().getReducingState(descriptor);
            }

            @Override
            public Tuple3<String, String, Long> map(Tuple3<String,
                    String, Long> value) throws Exception {

                Tuple2<String, Long> lastAreaTimes = stayAreaTimeReducing.get() == null ? new Tuple2<>(value.f1, 0L) : stayAreaTimeReducing.get();

                stayAreaTimeReducing.add(new Tuple2<>(value.f1, lastAreaTimes.f1));

                Tuple3<String, String, Long> result = new Tuple3<>();

                Tuple2<String, Long> currentAreaTimes = stayAreaTimeReducing.get();

                result.setFields(value.f0, value.f1, currentAreaTimes.f1);
                return result;
            }
        }).print();

        env.execute("StayTimeListState demo");
    }
}
