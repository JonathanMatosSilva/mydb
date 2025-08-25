package br.com.mydb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableFormatter {

    public static void printTable(List<Column> schema, List<Row> rows) {
        Map<String, Integer> columnWidths = new HashMap<>();

        for (Column col : schema) {
            columnWidths.put(col.name(), col.name().length());
        }

        for (Row row : rows) {
            for (Column col : schema) {
                Object value = row.get(col.name());
                if (value != null) {
                    int valueWidth = String.valueOf(value).length();
                    if (valueWidth > columnWidths.get(col.name())) {
                        columnWidths.put(col.name(), valueWidth);
                    }
                }
            }
        }

        StringBuilder separator = new StringBuilder();
        for (Column col : schema) {
            separator.append("+");
            separator.append("-".repeat(columnWidths.get(col.name()) + 2));
        }
        separator.append("+");

        StringBuilder header = new StringBuilder();
        for (Column col : schema) {
            int width = columnWidths.get(col.name());
            header.append(String.format("| %-" + width + "s ", col.name()));
        }
        header.append("|");

        System.out.println(separator);
        System.out.println(header);
        System.out.println(separator);

        for (Row row : rows) {
            StringBuilder rowLine = new StringBuilder();
            for (Column col : schema) {
                Object value = row.get(col.name());
                int width = columnWidths.get(col.name());
                rowLine.append(String.format("| %-" + width + "s ", value));
            }
            rowLine.append("|");
            System.out.println(rowLine);
        }
        System.out.println(separator);
        System.out.println(rows.size() + " linha(s) encontradas.");
    }
}