package com.woople.streaming.state;

import com.google.common.collect.Lists;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

public class StayTimeListState01 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = env.addSource(new StateDataSource()).keyBy(0);

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple4<String, String, Long, Integer>>() {
            private ListState<Tuple2<String, Long>> stayAreaTimeList;// area, first entry time

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<Tuple2<String, Long>> descriptor =
                        new ListStateDescriptor<>(
                                "stayAreaTime",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));
                stayAreaTimeList = getRuntimeContext().getListState(descriptor);
            }

            @Override
            public Tuple4<String, String, Long, Integer> map(Tuple3<String, String, Long> value) throws Exception {

                Iterable<Tuple2<String, Long>> areaTimeIterable = stayAreaTimeList.get();
                List<Tuple2<String, Long>> currentStayAreaTimeList = ListUtils.EMPTY_LIST;

                if (areaTimeIterable != null){
                    currentStayAreaTimeList = Lists.newArrayList(areaTimeIterable);
                }

                String currentArea = value.f1;
                int times = 0;

                System.out.println(currentStayAreaTimeList);

                for (int i = 0; i < currentStayAreaTimeList.size(); i++) {
                    if (currentStayAreaTimeList.get(i).f0.equals(currentArea)){
                        times = times + 1;
                    }
                }

                Tuple4<String, String, Long, Integer> result = new Tuple4<>();

                if (currentStayAreaTimeList.size() > 0 && value.f1.equals(currentStayAreaTimeList.get(currentStayAreaTimeList.size()-1).f0)){
                    result.setFields(value.f0, value.f1, value.f2 - currentStayAreaTimeList.get(currentStayAreaTimeList.size()-1).f1, times);
                }else {
                    result.setFields(value.f0, value.f1, 0L, times + 1);
                    stayAreaTimeList.add(new Tuple2<>(value.f1, value.f2));
                }

                return result;
            }
        }).print();

        env.execute("StayTimeListState demo");
    }
}
