package com.woople.streaming.state;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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
import java.util.List;

public class StayTimeListState {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //f0:手机号，f0:当前位置，f1:上报位置的时间
        KeyedStream<Tuple3<String, String, Long>, Tuple> keyedStream = env.addSource(new StateDataSource()).keyBy(0);

        keyedStream.map(new RichMapFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>>() {
            //当前区域的驻留时长, f0:上次位置，f1:上次的时间
            private ValueState<Tuple2<String, Long>> stayAreaTime;
            //历史轨迹每个区域的驻留时长，f0:位置，f1:驻留时间
            private ListState<Tuple2<String, Long>> stayAreaTimeHistory;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Tuple2<String, Long>> descriptor =
                        new ValueStateDescriptor<>(
                                "stayAreaTime",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));
                stayAreaTime = getRuntimeContext().getState(descriptor);

                ListStateDescriptor<Tuple2<String, Long>> historyDescriptor =
                        new ListStateDescriptor<>(
                                "stayAreaTimeHistory",
                                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                                }));
                stayAreaTimeHistory = getRuntimeContext().getListState(historyDescriptor);
            }

            @Override
            public Tuple3<String, String, Long> map(Tuple3<String, String, Long> value) throws Exception {
                Tuple2<String, Long> currentAreaTime = stayAreaTime.value();
                List<Tuple2<String, Long>> currentStayAreaTimeHistory = Lists.newArrayList(stayAreaTimeHistory.get());
                long historyTime = 0L;

                //遍历历史数据计算所有当前区域的驻留时长
                for (Tuple2<String, Long> history : currentStayAreaTimeHistory) {
                    if (history.f0.equals(value.f1)){
                        historyTime = historyTime + history.f1;
                    }
                }

                Tuple3<String, String, Long> result = new Tuple3<>();

                if (currentAreaTime != null && value.f1.equals(currentAreaTime.f0)){
                    long sum = value.f2 - currentAreaTime.f1 + historyTime;
                    result.setFields(value.f0, value.f1, sum);
                    stayAreaTimeHistory.add(new Tuple2<>(value.f1, value.f2 - currentAreaTime.f1));
                }else {
                    result.setFields(value.f0, value.f1, historyTime);
                }

                stayAreaTime.update(new Tuple2<>(value.f1, value.f2));
                return result;
            }
        }).print();

        env.execute("StayTimeListState demo");
    }
}
