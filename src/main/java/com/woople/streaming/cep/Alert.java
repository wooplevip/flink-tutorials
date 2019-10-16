package com.woople.streaming.cep;

import java.util.Objects;

public class Alert {
    private String id;
    private String level;

    public Alert(String id, String level) {
        this.id = id;
        this.level = level;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Alert alert = (Alert) o;
        return Objects.equals(id, alert.id) &&
                Objects.equals(level, alert.level);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, level);
    }

    @Override
    public String toString() {
        return "Alert{" +
                "id='" + id + '\'' +
                ", level='" + level + '\'' +
                '}';
    }
}
