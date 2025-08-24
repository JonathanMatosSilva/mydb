package br.com.mydb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RowSerializer {

    public static byte[] serialize(Row row, List<Column> schema) {
        List<byte[]> varLenData = new ArrayList<>();
        int fixedSizeTotal = 0;
        int varSizeTotal = 0;

        for (Column col : schema) {
            if (col.type().sizeInBytes > 0) {
                fixedSizeTotal += col.type().sizeInBytes;
            } else {
                fixedSizeTotal += 4;
                Object value = row.get(col.name());
                byte[] bytes = (value == null) ? new byte[0] : ((String) value).getBytes(StandardCharsets.UTF_8);
                varLenData.add(bytes);
                varSizeTotal += bytes.length;
            }
        }

        ByteBuffer buffer = ByteBuffer.allocate(fixedSizeTotal + varSizeTotal);
        int currentVarOffset = fixedSizeTotal;

        int varDataIndex = 0;
        for (Column col : schema) {
            Object value = row.get(col.name());
            if (value == null) {
                if(col.type() == DataType.INTEGER) buffer.putInt(0);
                if(col.type() == DataType.VARCHAR) buffer.putInt(0);
                continue;
            }

            switch (col.type()) {
                case INTEGER:
                    buffer.putInt((Integer) value);
                    break;
                case VARCHAR:
                    byte[] varBytes = varLenData.get(varDataIndex++);
                    buffer.putShort((short) currentVarOffset);
                    buffer.putShort((short) varBytes.length);
                    currentVarOffset += varBytes.length;
                    break;
            }
        }

        for (byte[] varBytes : varLenData) {
            buffer.put(varBytes);
        }

        return buffer.array();
    }

    public static Row deserialize(byte[] data, List<Column> schema) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        Row row = new Row();

        for (Column col : schema) {
            switch (col.type()) {
                case INTEGER:
                    row.put(col.name(), buffer.getInt());
                    break;
                case VARCHAR:
                    short offset = buffer.getShort();
                    short length = buffer.getShort();
                    byte[] strBytes = new byte[length];
                    int currentPosition = buffer.position();
                    buffer.position(offset);
                    buffer.get(strBytes, 0, length);
                    buffer.position(currentPosition);
                    row.put(col.name(), new String(strBytes, StandardCharsets.UTF_8));
                    break;
            }
        }
        return row;
    }
}
