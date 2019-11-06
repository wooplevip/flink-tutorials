package com.woople.streaming.base;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
 * The current total count is keyed state managed by Flink.
 */
public class RollingAdditionMapper extends RichMapFunction<KafkaEvent, KafkaEvent> {

	private static final long serialVersionUID = 1180234853172462378L;

	private transient ValueState<Integer> currentTotalCount;

	@Override
	public KafkaEvent map(KafkaEvent event) throws Exception {
		System.out.println(event + " ============ ");
		Integer totalCount = currentTotalCount.value();

		if (totalCount == null) {
			totalCount = 0;
		}
		totalCount += event.getFrequency();

		currentTotalCount.update(totalCount);

		return new KafkaEvent(event.getWord(), totalCount, event.getTimestamp());
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
	}
}
