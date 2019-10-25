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

	private static final long serialVersionUID = -742759155861320823L;

	private long currentTimestamp = Long.MIN_VALUE;

	@Override
	public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
		// the inputs are assumed to be of format (message,timestamp)
		this.currentTimestamp = event.getTimestamp();
		return event.getTimestamp();
	}

	@Nullable
	@Override
	public Watermark getCurrentWatermark() {
		return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
	}
}
