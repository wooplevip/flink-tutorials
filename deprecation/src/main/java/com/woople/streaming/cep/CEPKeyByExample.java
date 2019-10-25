package com.woople.streaming.cep;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CEPKeyByExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "host-10-1-236-139:6667");
        properties.setProperty("group.id", "cepG");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer010<>("foo", new SimpleStringSchema(), properties));

        DataStream<Event> input = stream.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) throws Exception {
                String[] v = value.split(",");
                return new Event(v[0], EventType.valueOf(v[1]), Double.parseDouble(v[2]));
            }
        });

    Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        System.out.println(event + " from start");
                        return event.getType() == EventType.VALID && event.getVolume() < 10;
                    }
                }
        ).followedBy("end").where(
                new SimpleCondition<Event>() {
                    @Override
                    public boolean filter(Event event) {
                        System.out.println(event + " from end");
                        return event.getType() == EventType.VALID && event.getVolume() > 100;
                    }
                }
        );

        PatternStream<Event> patternStream = CEP.pattern(input.keyBy(Event::getId), pattern);

        DataStream<Alert> result = patternStream.select((Map<String, List<Event>> p) -> {
            List<Event> first = p.get("start");
            List<Event> second = p.get("end");
            return new Alert("111", "CRITICAL");
        });

        result.print();

        env.execute("Flink cep example");

}

}
