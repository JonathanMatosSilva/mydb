package br.com.mydb;

import java.io.IOException;

public class Cursor {

    private final Table table;
    private int leafPageNumber;
    private int cellNumber;
    private boolean endOfTable;

    public Cursor(Table table, int startLeafPageNumber) {
        this.table = table;
        this.leafPageNumber = startLeafPageNumber;
        this.cellNumber = 0;

        try {
            Page startPage = table.getPage(startLeafPageNumber);
            BTreeNode startNode = new BTreeNode(startPage, Table.BTREE_MIN_DEGREE);
            this.endOfTable = (startNode.getKeyCount() == 0);
        } catch (IOException e) {
            this.endOfTable = true;
        }
    }

    public Row getRecord() throws IOException {
        Page leafPage = table.getPage(this.leafPageNumber);
        BTreeNode leafNode = new BTreeNode(leafPage, Table.BTREE_MIN_DEGREE);

        long dataPointer = leafNode.getDataPointer(this.cellNumber);

        int dataPageNumber = (int) (dataPointer >> 32);
        int dataSlotId = (int) (dataPointer);
        Page dataPage = this.table.getPage(dataPageNumber);
        byte[] rawRecord = dataPage.getRecord(dataSlotId);

        return RowSerializer.deserialize(rawRecord, table.getSchema());
    }

    public void advance() throws IOException {
        if (endOfTable) {
            return;
        }

        Page currentPage = table.getPage(this.leafPageNumber);
        BTreeNode currentNode = new BTreeNode(currentPage, Table.BTREE_MIN_DEGREE);

        this.cellNumber++;

        if (this.cellNumber < currentNode.getKeyCount()) {
            return;
        }

        int nextPageNumber = currentNode.getNextSiblingPointer();
        if (nextPageNumber == BTreeNode.NULL_POINTER) {
            this.endOfTable = true;
        } else {
            this.leafPageNumber = nextPageNumber;
            this.cellNumber = 0;
        }
    }

    public boolean isEndOfTable() {
        return endOfTable;
    }
}