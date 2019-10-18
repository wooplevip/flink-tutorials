package com.woople.streaming.cep;


import java.util.Objects;

public class SubEvent extends Event {
	private long timestamp;
	public SubEvent(String id, EventType type, double volume, long timestamp) {
		super(id, type, volume);
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "SubEvent{" +
				"timestamp=" + timestamp + ", time=" + StringUtilsPlus.stampToDate(timestamp) + ", "+ getId() + ", " + getType().name() + ", " + getVolume() +
				'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		SubEvent subEvent = (SubEvent) o;
		return timestamp == subEvent.timestamp;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), timestamp);
	}
}
