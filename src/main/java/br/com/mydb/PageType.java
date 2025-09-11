package br.com.mydb;

public enum PageType {

    DATA_PAGE((byte) 0x0D),
    BTREE_LEAF_NODE((byte) 0x00),
    BTREE_INTERNAL_NODE((byte) 0x01);

    public final byte value;

    PageType(byte value) {
        this.value = value;
    }
}