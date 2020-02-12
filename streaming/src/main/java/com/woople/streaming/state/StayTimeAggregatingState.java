package com.woople.streaming.state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StayTimeAggregatingState {
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

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple2<String, Double>>() {
            //输入为驻留时长，输出为平均驻留时长
            private AggregatingState<Long, Double> stayAreaTimeAgg;
            @Override
            public void open(Configuration parameters) throws Exception {
                AggregatingStateDescriptor<Long, AverageAccumulator, Double> aggDescriptor =
                        new AggregatingStateDescriptor<>(
                                "stayAreaTime",
                                new AggregateFunction<Long, AverageAccumulator, Double>() {

                                    @Override
                                    public AverageAccumulator createAccumulator() {
                                        return new AverageAccumulator();
                                    }

                                    @Override
                                    public AverageAccumulator add(Long value, AverageAccumulator acc) {
                                        acc.sum += value;
                                        acc.count++;
                                        return acc;
                                    }

                                    @Override
                                    public Double getResult(AverageAccumulator acc) {
                                        return acc.sum / (double) acc.count;
                                    }

                                    @Override
                                    public AverageAccumulator merge(AverageAccumulator a, AverageAccumulator b) {
                                        a.count += b.count;
                                        a.sum += b.sum;
                                        return a;
                                    }
                                },
                                TypeInformation.of(new TypeHint<AverageAccumulator>() {
                                }));
                stayAreaTimeAgg = getRuntimeContext().getAggregatingState(aggDescriptor);
            }

            @Override
            public Tuple2<String, Double> map(Tuple3<String, String, Long> value) throws Exception {
                stayAreaTimeAgg.add(value.f2);
                return new Tuple2<>(value.f0, stayAreaTimeAgg.get());
            }
        }).print();

        env.execute("StayTimeAggregatingState demo");
    }

    private static class AverageAccumulator {
        private long count;
        private long sum;
    }
}
