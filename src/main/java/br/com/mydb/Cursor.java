package br.com.mydb;

import java.io.IOException;

public class Cursor {

    private final Table table;
    private int pageNumber;
    private int cellNumber;
    private boolean endOfTable;

    public Cursor(Table table, int pageNumber, int cellNumber) {
        this.table = table;
        this.pageNumber = pageNumber;
        this.cellNumber = cellNumber;
        this.endOfTable = false;
    }

    // REWRITE
//    public User getValue() throws IOException {
//        Page leafPage = table.getPage(this.pageNumber);
//        BTreeNode leafNode = new BTreeNode(leafPage, Table.BTREE_MIN_DEGREE);
//
//        long dataOffset = leafNode.getDataPointer(this.cellNumber);
//        byte[] rowData = table.getPager().readBytesAt(dataOffset, User.ROW_SIZE);
//
//        return User.fromBytes(rowData);
//    }

    public void advance() throws IOException {
        if (endOfTable) {
            return;
        }

        Page currentPage = table.getPage(this.pageNumber);
        this.cellNumber++;

        if (this.cellNumber < currentPage.getRowCount()) {
            return;
        }

        this.pageNumber++;
        this.cellNumber = 0;

        if (this.pageNumber >= table.getNumPages()) {
            this.endOfTable = true;
            return;
        }
    }

    public boolean isEndOfTable() {
        return endOfTable;
    }
}