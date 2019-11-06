package com.woople.streaming.base;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
 * records are strictly ascending.
 *
 * <p>Flink also ships some built-in convenience assigners, such as the
 * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
 */
public class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

	private final long maxOutOfOrderness = 5000;

	private long currentMaxTimestamp;

	@Override
	public long extractTimestamp(KafkaEvent kafkaEvent, long previousElementTimestamp) {
		System.out.println("kafkaEvent is " + kafkaEvent);
		long timestamp = kafkaEvent.getTimestamp();
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		System.out.println("watermark:" + String.valueOf(currentMaxTimestamp - maxOutOfOrderness));
		return timestamp;
	}

	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}
}
