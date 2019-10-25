package com.woople.streaming.base;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaExample{

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend("file:///Users/tmp/checkpoint", true));
		env.setParallelism(1).enableCheckpointing(5000); // create a checkpoint every 5 seconds

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "host-10-1-236-139:6667");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		DataStream<KafkaEvent> input = env
			.addSource(
				new FlinkKafkaConsumer<>(
					"foo",
					new KafkaEventSchema(),
						properties)
					.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
			.keyBy("word")
			.map(new RollingAdditionMapper()
			);

		input.addSink(
			new FlinkKafkaProducer<>(
				"bar",
				new KafkaSerializationSchemaImpl(),
					properties,
				FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));

		env.execute("Modern Kafka Example");
	}

}
