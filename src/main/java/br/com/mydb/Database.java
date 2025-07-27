package br.com.mydb;

import java.io.IOException;

public class Database {
    private final Pager pager;

    public Database(String databaseFilePath) throws IOException {
        this.pager = new Pager(databaseFilePath, 4096);
    }

    public Table openTable() throws IOException {
        if (pager.getNumPages() == 0) {
            Page page = pager.newPage();
            page.setPageType(PageType.BTREE_LEAF_NODE.value);
            page.setRowCount(0);
            int rootPageNumber = 0;

            BTreeNode rootNode = new BTreeNode(page, Table.BTREE_MIN_DEGREE);
            rootNode.setNextSiblingPointer(BTreeNode.NULL_POINTER);

            return new Table(this.pager, rootPageNumber);
        }
        int rootPageNumber = readRootPageNumberFromHeader();
        return new Table(this.pager, rootPageNumber);
    }

    private int readRootPageNumberFromHeader() throws IOException {
        // Page headerPage = pager.getPage(0);
        // return headerPage.readIntAt(offset_root_pointer);
        return 0;
    }

    public void close() throws IOException {
        pager.close();
    }
}
