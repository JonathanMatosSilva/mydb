package br.com.mydb;

import java.nio.ByteBuffer;

public class BTreeNode {

    private final Page page;
    private final int order;

    private static final int KEY_SIZE = 4;
    private static final int POINTER_SIZE = 8;
    private static final int CELL_SIZE = KEY_SIZE + POINTER_SIZE;
    
    private static final int CHILD_POINTER_SIZE = 4;
    private static final int CHILD_SIZE = KEY_SIZE + CHILD_POINTER_SIZE;

    public BTreeNode(Page pager, int order) {
        this.page = pager;
        this.order = order;
    }

    public boolean isLeaf() {
        return this.page.getPageType() == PageType.BTREE_LEAF_NODE.value;
    }

    private int getCellOffset(int index) {
        if (!isLeaf()) {
            return Page.HEADER_SIZE + (index * CHILD_SIZE);

        }
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

    public int getKeyCount() {
        return this.page.getRowCount();
    }

    public void setKeyCount(int count) {
        this.page.setRowCount(count);
    }

    public int getPageNumber() {
        return this.page.getPageNumber();
    }

    public long getDataPointer(int index) {
        if (!isLeaf()) {
            throw new IllegalStateException("Não se pode obter um ponteiro de dados de um nó interno.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        return buffer.getLong(getCellOffset(index) + KEY_SIZE);
    }

    public void setDataPointer(int index, long pointer) {
        if (!isLeaf()) {
            throw new IllegalStateException("Não se pode definir um ponteiro de dados em um nó interno.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        buffer.putLong(getCellOffset(index) + KEY_SIZE, pointer);
        this.page.markAsDirty();
    }

    public int getChildPointer(int index) {
        if (isLeaf()) {
            throw new IllegalStateException("Não se pode obter um filho de um nó folha.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        return buffer.getInt(getCellOffset(index) + KEY_SIZE);
    }

    public void setChildPointer(int index, int pageNumber) {
        if (isLeaf()) {
            throw new IllegalStateException("Não se pode definir um filho em um nó folha.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        buffer.putInt(getCellOffset(index) + KEY_SIZE, pageNumber);
        page.markAsDirty();
    }

    public Page getPage() {
        return this.page;
    }

    public int getNextSiblingPointer() {
        if (!isLeaf()) {
            throw new IllegalStateException("Apenas nós folha possuem ponteiros para irmãos.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        return buffer.getInt(Page.NEXT_SIBLING_POINTER_OFFSET);
    }

    public void setNextSiblingPointer(int pageNumber) {
        if (!isLeaf()) {
            throw new IllegalStateException("Apenas nós folha possuem ponteiros para irmãos.");
        }
        ByteBuffer buffer = ByteBuffer.wrap(this.page.getBytes());
        buffer.putInt(Page.NEXT_SIBLING_POINTER_OFFSET, pageNumber);
        page.markAsDirty();
    }
}
