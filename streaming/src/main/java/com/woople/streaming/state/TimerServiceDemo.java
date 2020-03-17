package com.woople.streaming.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

public class TimerServiceDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<Tuple3<String, Long, Long>> stream = env.addSource(new DataSource());

        stream.keyBy(0)
                .process(new CountWithTimeoutFunction()).print("Result: ");

        env.execute("TimerServiceDemo");
    }

    public static class CountWithTimeoutFunction
            extends KeyedProcessFunction<Tuple, Tuple3<String, Long, Long>, Tuple3<String, Long, Long>> {

        /** The state that is maintained by this process function */
        private ValueState<Tuple3<String, Long, Long>> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState",TypeInformation.of(
                    new TypeHint<Tuple3<String, Long, Long>>() {
            })));
        }

        @Override
        public void processElement(
                Tuple3<String, Long, Long> value,
                Context ctx,
                Collector<Tuple3<String, Long, Long>> out) throws Exception {

            // write the state back
            state.update(value);

            // schedule the next timer 60 seconds from the current event time
            ctx.timerService().registerProcessingTimeTimer(value.f2);

            out.collect(value);
        }

        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<Tuple3<String, Long, Long>> out) throws Exception {

            System.out.println("Current time:" + System.currentTimeMillis());
            // get the state for the key that scheduled the timer
            Tuple3<String, Long, Long> result = state.value();
            result.setField("bar", 0);
            out.collect(result);
        }
    }


    private static class DataSource extends RichParallelSourceFunction<Tuple3<String, Long, Long>> {

        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple3<String, Long, Long>> ctx) throws Exception {
            Tuple3[] data = new Tuple3[]{
                    new Tuple3<>("foo", System.currentTimeMillis(), System.currentTimeMillis() + 5000),
                    new Tuple3<>("foo", System.currentTimeMillis(), System.currentTimeMillis() + 10000)};
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
