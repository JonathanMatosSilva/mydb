package br.com.mydb;

import java.nio.ByteBuffer;

public class Page {

    private static final int PAGE_TYPE_OFFSET = 0; // 1 byte
    private static final int ROW_COUNT_OFFSET = 1; // 2 bytes
    public static final int NEXT_SIBLING_POINTER_OFFSET = 3; // 4 bytes
    public static final int NEXT_DATA_PAGE_POINTER_OFFSET = 7; // 4 bytes

    public static final int HEADER_SIZE = 1 + 2 + 4 + 4; // 11 bytes

    private final int pageNumber;
    private final byte[] data;
    private boolean isDirty;


    public Page(int pageNumber, byte[] data) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.isDirty = false;
    }

    public byte getPageType() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.get(PAGE_TYPE_OFFSET);
    }

    public void setPageType(byte type) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.put(PAGE_TYPE_OFFSET, type);
    }

    public int getRowCount() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getShort(ROW_COUNT_OFFSET);
    }

    public void setRowCount(int count) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putShort(ROW_COUNT_OFFSET, (short) count);
        this.isDirty = true;
    }

    public byte[] getRow(int slot, int rowSize) {
        int offset = HEADER_SIZE + (slot * rowSize);
        byte[] rowData = new byte[rowSize];
        System.arraycopy(this.data, offset, rowData, 0, rowSize);
        return rowData;
    }

    public void setRow(int slot, byte[] rowData, int rowSize) {
        int offset = HEADER_SIZE + (slot * rowSize);
        System.arraycopy(rowData, 0, this.data, offset, rowSize);
    }

    public int getPageNumber() {
        return this.pageNumber;
    }

    public boolean isDirty() {
        return this.isDirty;
    }

    public byte[] getBytes() {
        return this.data;
    }

    public void markAsDirty() {
        this.isDirty = true;
    }

    public int getNextDataPagePointer() {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return buffer.getInt(NEXT_DATA_PAGE_POINTER_OFFSET);
    }

    public void setNextDataPagePointer(int pageNumber) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.putInt(NEXT_DATA_PAGE_POINTER_OFFSET, pageNumber);
        this.isDirty = true;
    }

}