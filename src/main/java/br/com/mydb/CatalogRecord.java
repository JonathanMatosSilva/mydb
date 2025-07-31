package br.com.mydb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CatalogRecord {

    public static final int TABLE_NAME_SIZE = 32;
    public static final int ROOT_PAGE_NUM_SIZE = 4;
    public static final int TABLE_ROW_SIZE = 4;
    public static final int FIRST_DATA_PAGE_NUM_SIZE = 4;
    public static final int ROW_SIZE = TABLE_NAME_SIZE + ROOT_PAGE_NUM_SIZE + FIRST_DATA_PAGE_NUM_SIZE + TABLE_ROW_SIZE;

    private final String tableName;
    private final int rootPageNumber;
    private final int firstDataPageNumber; // NOVO CAMPO
    private final int rowSize;

    public CatalogRecord(String tableName, int rootPageNumber, int firstDataPageNumber, int rowSize) {
        this.tableName = tableName;
        this.rootPageNumber = rootPageNumber;
        this.firstDataPageNumber = firstDataPageNumber;
        this.rowSize = rowSize;
    }


    public String getTableName() { return tableName; }
    public int getRootPageNumber() { return rootPageNumber; }
    public int getFirstDataPageNumber() { return firstDataPageNumber; }
    public int getRowSize() { return rowSize; }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(ROW_SIZE);

        byte[] nameBytes = new byte[TABLE_NAME_SIZE];
        byte[] sourceNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(sourceNameBytes, 0, nameBytes, 0, sourceNameBytes.length);

        buffer.put(nameBytes);
        buffer.putInt(this.rootPageNumber);
        buffer.putInt(this.firstDataPageNumber);
        buffer.putInt(this.rowSize);
        return buffer.array();
    }

    public static CatalogRecord fromBytes(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] nameBytes = new byte[TABLE_NAME_SIZE];
        buffer.get(nameBytes);
        String tableName = new String(nameBytes, StandardCharsets.UTF_8).trim();
        int rootPageNumber = buffer.getInt();
        int firstDataPageNumber = buffer.getInt();
        int rowSize = buffer.getInt();
        return new CatalogRecord(tableName, rootPageNumber, firstDataPageNumber, rowSize);
    }

    @Override
    public String toString() {
        return "CatalogRecord{" +
                "tableName='" + tableName + '\'' +
                ", rootPageNumber=" + rootPageNumber +
                ", firstDataPageNumber=" + firstDataPageNumber +
                ", rowSize=" + rowSize +
                '}';
    }
}