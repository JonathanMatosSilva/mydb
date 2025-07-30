package br.com.mydb;

import java.nio.ByteBuffer;

public class BTreeNode {

    private final Page page;
    private final int order;

    public static final int NULL_POINTER = -1;

    private static final int KEY_SIZE = 4;
    private static final int POINTER_SIZE = 8;
    private static final int CHILD_POINTER_SIZE = 4;

    public BTreeNode(Page pager, int order) {
        this.page = pager;
        this.order = order;
    }

    public boolean isLeaf() {
        return this.page.getPageType() == PageType.BTREE_LEAF_NODE.value;
    }

    private int getLeafCellSize() {
        return KEY_SIZE + POINTER_SIZE;
    }

    private int getLeafCellOffset(int index) {
        return Page.HEADER_SIZE + (index * getLeafCellSize());
    }

    private int getLeafKeyOffset(int index) {
        return getLeafCellOffset(index);
    }

    private int getLeafDataPointerOffset(int index) {
        return getLeafCellOffset(index) + KEY_SIZE;
    }

    private int getInternalChildPointerOffset(int index) {
        return Page.HEADER_SIZE + (index * (KEY_SIZE + CHILD_POINTER_SIZE));
    }

    private int getInternalKeyOffset(int index) {
        return getInternalChildPointerOffset(index) + CHILD_POINTER_SIZE;
    }

    public int getKey(int index) {
        int offset = isLeaf() ? getLeafKeyOffset(index) : getInternalKeyOffset(index);
        return ByteBuffer.wrap(this.page.getBytes()).getInt(offset);
    }

    public void setKey(int index, int key) {
        int offset = isLeaf() ? getLeafKeyOffset(index) : getInternalKeyOffset(index);
        ByteBuffer.wrap(this.page.getBytes()).putInt(offset, key);
        this.page.markAsDirty();
    }

    public long getDataPointer(int index) {
        if (!isLeaf()) {
            throw new IllegalStateException("Não se pode obter um ponteiro de dados de um nó interno.");
        }
        int offset = getLeafDataPointerOffset(index);
        return ByteBuffer.wrap(this.page.getBytes()).getLong(offset);
    }

    public void setDataPointer(int index, long pointer) {
        if (!isLeaf()) {
            throw new IllegalStateException("Não se pode definir um ponteiro de dados em um nó interno.");
        }
        int offset = getLeafDataPointerOffset(index);
        ByteBuffer.wrap(this.page.getBytes()).putLong(offset, pointer);
        this.page.markAsDirty();
    }

    public int getChildPointer(int index) {
        if (isLeaf()) {
            throw new IllegalStateException("Não se pode obter um filho de um nó folha.");
        }
        int offset = getInternalChildPointerOffset(index);
        return ByteBuffer.wrap(this.page.getBytes()).getInt(offset);
    }

    public void setChildPointer(int index, int pageNumber) {
        if (isLeaf()) {
            throw new IllegalStateException("Não se pode definir um filho em um nó folha.");
        }
        int offset = getInternalChildPointerOffset(index);
        ByteBuffer.wrap(this.page.getBytes()).putInt(offset, pageNumber);
        page.markAsDirty();
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

    public int getKeyCount() {
        return this.page.getRowCount();
    }

    public void setKeyCount(int count) {
        this.page.setRowCount(count);
    }

    public int getPageNumber() {
        return this.page.getPageNumber();
    }

    public Page getPage() {
        return this.page;
    }

}
