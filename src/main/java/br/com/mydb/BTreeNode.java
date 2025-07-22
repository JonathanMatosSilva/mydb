package br.com.mydb;

import java.nio.ByteBuffer;

public class BTreeNode {

    private final Page page;
    private final int order;

    private static final int KEY_SIZE = 4;   // int
    private static final int POINTER_SIZE = 8; // long
    private static final int CELL_SIZE = KEY_SIZE + POINTER_SIZE;

    public BTreeNode(Page pager, int order) {
        this.page = pager;
        this.order = order;
    }

    private int getCellOffset(int index) {
        return Page.HEADER_SIZE + (index * CELL_SIZE);
    }

    public int getKey(int index) {
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        return buffer.getInt(getCellOffset(index));
    }

    public void setKey(int index, int key) {
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        buffer.putInt(getCellOffset(index), key);
        this.page.markAsDirty();
    }

    public long getDataPointer(int index) {
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        return buffer.getLong(getCellOffset(index) + KEY_SIZE);
    }

    public void setDataPointer(int index, long pointer) {
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        buffer.putLong(getCellOffset(index) + KEY_SIZE, pointer);
        this.page.markAsDirty();
    }

    public int getKeyCount() {
        return this.page.getRowCount();
    }

    public void setKeyCount(int count) {
        this.page.setRowCount(count);
    }
}
