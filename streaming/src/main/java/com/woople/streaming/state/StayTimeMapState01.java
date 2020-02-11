package com.woople.streaming.state;

import com.google.common.collect.Lists;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class StayTimeMapState01 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = env.addSource(new StateDataSource()).keyBy(0);

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            private MapState<String, Tuple2<String, Long>> stayAreaTimeMap;// area, first entry time

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Tuple2<String, Long>> descriptor =
                        new MapStateDescriptor<>(
                                "stayAreaTime",
                                TypeInformation.of(new TypeHint<String>() {
                                }),
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));
                stayAreaTimeMap = getRuntimeContext().getMapState(descriptor);
            }

            @Override
            public Tuple3<String, String, Long> map(Tuple3<String, String, Long> value) throws Exception {
                String lastArea = "";
                long lastAreaStartTime = 0L;
                Tuple2<String, Long> lastAreaTime = stayAreaTimeMap.get("lastArea");
                if (lastAreaTime == null){
                    lastArea = value.f1;
                    lastAreaStartTime = value.f2;
                    stayAreaTimeMap.put("lastArea", new Tuple2<>(value.f1, value.f2));
                }else {
                    lastArea = lastAreaTime.f0;
                    lastAreaStartTime = lastAreaTime.f1;
                }

                Tuple2<String, Long> currentAreaSumTuple = stayAreaTimeMap.get(value.f1);
                long currentAreaSum = 0L;

                if (currentAreaSumTuple != null){
                    currentAreaSum = currentAreaSumTuple.f1;
                }else {
                    stayAreaTimeMap.put(value.f1, new Tuple2<>(value.f1, currentAreaSum));
                }

                Tuple3<String, String, Long> result = new Tuple3<>();

                if (lastArea.equals(value.f1)){
                    currentAreaSum = currentAreaSum + (value.f2 - lastAreaStartTime);
                    stayAreaTimeMap.put(value.f1, new Tuple2<>(value.f1, currentAreaSum));
                }else {
                    stayAreaTimeMap.put("lastArea", new Tuple2<>(value.f1, value.f2));
                }

                result.setFields(value.f0, value.f1, currentAreaSum);
                return result;
            }
        }).print();

        env.execute("StayTimeMapState demo");
    }
}
