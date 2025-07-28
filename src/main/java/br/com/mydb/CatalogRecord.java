package br.com.mydb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CatalogRecord {

    public static final int TABLE_NAME_SIZE = 32;
    public static final int ROOT_PAGE_NUM_SIZE = 4;
    public static final int ROW_SIZE = TABLE_NAME_SIZE + ROOT_PAGE_NUM_SIZE;

    private String tableName;
    private int rootPageNumber;

    public CatalogRecord(String tableName, int rootPageNumber) {
        this.tableName = tableName;
        this.rootPageNumber = rootPageNumber;
    }


    public String getTableName() { return tableName; }
    public int getRootPageNumber() { return rootPageNumber; }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(ROW_SIZE);

        byte[] nameBytes = new byte[TABLE_NAME_SIZE];
        byte[] sourceNameBytes = this.tableName.getBytes(StandardCharsets.UTF_8);
        System.arraycopy(sourceNameBytes, 0, nameBytes, 0, sourceNameBytes.length);

        buffer.put(nameBytes);
        buffer.putInt(this.rootPageNumber);
        return buffer.array();
    }

    public static CatalogRecord fromBytes(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] nameBytes = new byte[TABLE_NAME_SIZE];
        buffer.get(nameBytes);
        String tableName = new String(nameBytes, StandardCharsets.UTF_8).trim();
        int rootPageNumber = buffer.getInt();
        return new CatalogRecord(tableName, rootPageNumber);
    }
}