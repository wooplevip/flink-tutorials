package com.woople.streaming.operators;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 手机缴费成功后，2秒内生效
 * key为订单编号，time为缴费时间，缴费成功后会收到一条数据
 * 缴费生效之后同样会收到一条数据，key为订单编号，time为生效时间
 * 输出2秒内没有生效的订单
 */

public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Tuple2<String, Long>> stream = env.addSource(new DataSource());

        stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        }).keyBy(0)
                .process(new TimeoutFunction()).print("Result: ");

        env.execute("ProcessFunctionDemo");
    }

    public static class TimeoutFunction
            extends KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple3<String, Long, Integer>> {
        //订单编号，缴费时间，生效时间，是否按时生效：0表示未按时、1表示按时
        private ValueState<Tuple4<String, Long, Long, Integer>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", TypeInformation.of(
                    new TypeHint<Tuple4<String, Long, Long, Integer>>() {
                    })));
        }

        @Override
        public void processElement(
                Tuple2<String, Long> value,
                Context ctx,
                Collector<Tuple3<String, Long, Integer>> out) throws Exception {

            Tuple4<String, Long, Long, Integer> history = state.value();
            if (history == null) {
                state.update(new Tuple4<>(value.f0, value.f1, -1L, 0));
                ctx.timerService().registerEventTimeTimer(value.f1 + 2000);
            } else {
                if (history.f3 == 0 && value.f1 - history.f1 <= 2000) {
                    state.update(new Tuple4<>(history.f0, history.f1, value.f1, 1));
                }
            }
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple3<String, Long, Integer>> out) throws Exception {

            Tuple4<String, Long, Long, Integer> result = state.value();
            //if (result.f3 == 0) {
                out.collect(new Tuple3<>(result.f0, result.f1, result.f3));
            //}
        }
    }


    private static class DataSource extends RichParallelSourceFunction<Tuple2<String, Long>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {
            Tuple2[] data = new Tuple2[]{
                    new Tuple2<>("1", 1584378010000L),//2020-03-17 01:00:10
                    new Tuple2<>("2", 1584378009000L),//2020-03-17 01:00:09
                    new Tuple2<>("1", 1584378012000L)
            };
            final long numElements = data.length;
            int i = 0;
            while (running && i < numElements) {
                ctx.collect(data[i]);
                System.out.println("sand data:" + data[i]);
                i++;
                Thread.sleep(1000);
            }
            Thread.sleep(90000);
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
