package com.woople.streaming.operators;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};
        SingleOutputStreamOperator<Integer> myInts = env.fromElements(1, 2, 3, 4, 5)
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
                        out.collect(value);

                        if (value % 2 == 0){
                            ctx.output(outputTag, "sideout-" + String.valueOf(value));
                        }
                    }
                });

        myInts.print("map");


        DataStream<String> sideOutputStream = myInts.getSideOutput(outputTag);
        sideOutputStream.print("SideOutput");
        env.execute("SideOutput Demo");
    }
}
