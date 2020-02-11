package com.woople.streaming.state;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StayTimeReducingState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = env.addSource(new StateDataSource()).keyBy(0);

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            private ValueState<Tuple2<String, Long>> stayAreaTime;// area and first entry time
            private ReducingState<Long> stayAreaTimeSum;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<String, Long>> descriptor =
                        new ValueStateDescriptor<>(
                                "stayAreaTime",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));
                stayAreaTime = getRuntimeContext().getState(descriptor);

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
                Tuple2<String, Long> currentAreaTime = stayAreaTime.value();
                long timeSum = stayAreaTimeSum.get();

                Tuple3<String, String, Long> result = new Tuple3<>();

                if (currentAreaTime != null && value.f1.equals("A")){
                    result.setFields(value.f0, value.f1, value.f2 - currentAreaTime.f1 + timeSum);
                    stayAreaTimeSum.add(value.f2 - currentAreaTime.f1);
                }else {
                    result.setFields(value.f0, value.f1, -1L);
                    stayAreaTime.update(new Tuple2<>(value.f1, value.f2));
                }

                return result;
            }
        }).print();

        env.execute("StayTimeValueState demo");
    }
}
