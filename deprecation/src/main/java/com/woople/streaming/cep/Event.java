package com.woople.streaming.cep;
import java.util.Objects;

public class Event {
	private EventType type;
	private double volume;
	private String id;

	public Event(String id, EventType type, double volume) {
		this.id = id;
		this.type = type;
		this.volume = volume;
	}

	public double getVolume() {
		return volume;
	}

	public String getId() {
		return id;
	}

	public EventType getType() {
		return type;
	}

	@Override
	public String toString() {
		return "Event(" + id + ", " + type.name() + ", " + volume + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Event) {
			Event other = (Event) obj;

			return type.name().equals(other.type.name()) && volume == other.volume && id.equals(other.id);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(type.name(), volume, id);
	}
}
