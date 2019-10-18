package com.woople.streaming.cep;


import java.util.Objects;

public class SubEvent extends Event {
	private String date;

	public SubEvent(String id, EventType type, double volume, String date) {
		super(id, type, volume);
		this.date = date;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		if (!super.equals(o)) return false;
		SubEvent subEvent = (SubEvent) o;
		return date.equals(subEvent.date);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), date);
	}

	@Override
	public String toString() {
		return "SubEvent{" +
				"date='" + date + '\'' + ", " + getId() + ", " + getType() + ", " + getVolume() +
				'}';
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}
