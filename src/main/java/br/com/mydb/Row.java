package br.com.mydb;

import java.util.HashMap;
import java.util.Map;

public class Row {
    private final Map<String, Object> values;

    public Row() {
        this.values = new HashMap<>();
    }

    public void put(String columnName, Object value) {
        values.put(columnName, value);
    }

    public Object get(String columnName) {
        return values.get(columnName);
    }

    public Map<String, Object> getValues() {
        return values;
    }

    @Override
    public String toString() {
        return values.toString();
    }
}