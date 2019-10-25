package com.woople.streaming.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CEPWithTimeExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");
        properties.setProperty("group.id", "cepG");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("foo", new SimpleStringSchema(), properties));


        DataStream<SubEvent> input = stream.map(new MapFunction<String, SubEvent>() {
            @Override
            public SubEvent map(String value) throws Exception {
                String[] v = value.split(",");
                return new SubEvent(v[0], EventType.valueOf(v[1]), Double.parseDouble(v[2]), v[3]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator());

        Pattern<SubEvent, ?> pattern = Pattern.<SubEvent>begin("start").where(
                new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent subEvent) {
                        System.out.println(subEvent + " from start at " + StringUtilsPlus.stampToDate(System.currentTimeMillis()));
                        return subEvent.getType() == EventType.VALID && subEvent.getVolume() < 10;
                    }
                }
        ).next("end").where(
                new SimpleCondition<SubEvent>() {
                    @Override
                    public boolean filter(SubEvent subEvent) {
                        System.out.println(subEvent + " from end");
                        return subEvent.getType() == EventType.VALID && subEvent.getVolume() > 100;
                    }
                }
        ).within(Time.seconds(10));

        PatternStream<SubEvent> patternStream = CEP.pattern(input, pattern);

        DataStream<Alert> result = patternStream.process(
                new PatternProcessFunction<SubEvent, Alert>() {
                    @Override
                    public void processMatch(
                            Map<String, List<SubEvent>> pattern,
                            Context ctx,
                            Collector<Alert> out) throws Exception {

                        System.out.println(pattern);

                        out.collect(new Alert("111", "CRITICAL"));
                    }
                });

        result.print();

        env.execute("Flink cep example");
    }

    private static class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<SubEvent> {

        private final long maxOutOfOrderness = 5000;

        private long currentMaxTimestamp;

        @Override
        public long extractTimestamp(SubEvent subEvent, long previousElementTimestamp) {
            System.out.println("SubEvent is " + subEvent);
            long timestamp = StringUtilsPlus.dateToStamp(subEvent.getDate());
            currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
            System.out.println("watermark:" + StringUtilsPlus.stampToDate(String.valueOf(currentMaxTimestamp - maxOutOfOrderness)));
            return timestamp;
        }

        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }
    }
}
