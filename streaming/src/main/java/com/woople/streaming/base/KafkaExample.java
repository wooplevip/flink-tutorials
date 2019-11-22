package com.woople.streaming.base;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaExample{

	public static void main(String[] args) throws Exception {
		long checkpointInterval = 10000;
		if (args !=null && args.length > 0 &&StringUtils.isNotEmpty(args[0])){
			checkpointInterval = Long.parseLong(args[0]);
		}

		final ParameterTool params = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend("file:///tmp/checkpoint", true));
		env.setParallelism(1);//
		env.enableCheckpointing(checkpointInterval); // create a checkpoint every 5 seconds
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.getConfig().setGlobalJobParameters(params);

		Properties properties = new Properties();

		properties.put("bootstrap.servers", "yj01:9092");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.setProperty("group.id", "eos");

		DataStream<KafkaEvent> input = env.addSource(
				new FlinkKafkaConsumer<>("foo", new KafkaEventSchema(), properties)
					.assignTimestampsAndWatermarks(new CustomWatermarkExtractor())).name("Example Source")
			//.keyBy("word")
			.map(new MapFunction<KafkaEvent, KafkaEvent>() {
				@Override
				public KafkaEvent map(KafkaEvent value) throws Exception {
					value.setFrequency(value.getFrequency() + 1);
					return value;
				}
			});
		properties.put("transaction.timeout.ms", "60000");
		input.addSink(
			new FlinkKafkaProducer<>(
				"baz",
				new KafkaSerializationSchemaImpl("baz"),
					properties,
				FlinkKafkaProducer.Semantic.EXACTLY_ONCE)).name("Example Sink");

		env.execute("Modern Kafka Example");
	}

}
