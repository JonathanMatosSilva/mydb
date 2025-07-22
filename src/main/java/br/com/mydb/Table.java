package br.com.mydb;

import java.io.IOException;

public class Table {

    private final Pager pager;
    private int rootPageNumber;

    private final int maxRowsPerPage;

    private static final int BTREE_ORDER = 5;

    public Table(Pager pager, int rootPageNumber) {
        this.pager = pager;
        this.rootPageNumber = rootPageNumber;
        this.maxRowsPerPage = (pager.getPageSize() - Page.HEADER_SIZE) / User.ROW_SIZE;
    }

    public void insert(User user) throws IOException {
        byte[] rowBytes = user.toBytes();
        long dataOffset;

        Page dataPage = findDataPageWithSpace();

        int slotData = dataPage.getRowCount();
        dataPage.setRow(slotData, rowBytes);
        dataPage.setRowCount(slotData + 1);

        long offsetInPage =  Page.HEADER_SIZE + ((long) slotData * User.ROW_SIZE);
        dataOffset = ((long) dataPage.getPageNumber() * this.pager.getPageSize()) + offsetInPage;

        this.pager.flushPage(dataPage);

        // insere btree

        Page rootPage = this.pager.getPage(this.rootPageNumber);
        BTreeNode rootNode = new BTreeNode(rootPage, BTREE_ORDER);

        int keyToInsert = user.getId();
        int currentKeyCount = rootNode.getKeyCount();

        int insertionPoint = 0;
        while (insertionPoint < currentKeyCount && rootNode.getKey(insertionPoint) < keyToInsert) {
            insertionPoint++;
        }

        for (int i = currentKeyCount; i > insertionPoint; i--) {
            rootNode.setKey(i, rootNode.getKey(i - 1));
            rootNode.setDataPointer(i, rootNode.getDataPointer(i - 1));
        }

        rootNode.setKey(insertionPoint, keyToInsert);
        rootNode.setDataPointer(insertionPoint, dataOffset);
        rootNode.setKeyCount(currentKeyCount + 1);
        pager.flushPage(rootPage);
    }

    public Cursor find(int key) throws IOException {
        Cursor cursor = this.start();

        while (!cursor.isEndOfTable()) {
            User currentRow = cursor.getValue();
            if (currentRow.getId() == key) {
                return cursor;
            }
            cursor.advance();
        }
        return null;
    }

    public Cursor start() {
        return new Cursor(this, 0, 0);
    }


    public Page getPage(int pageNumber) throws IOException {
        return this.pager.getPage(pageNumber);
    }

    public int getNumPages() {
        return this.pager.getNumPages();
    }

    private Page findDataPageWithSpace() throws IOException {
        int numPages = this.pager.getNumPages();
        for (int i = numPages - 1; i >= 0; i--) {
            Page candidatePage = this.pager.getPage(i);
            if (candidatePage.getPageType() == PageType.DATA_PAGE.value) {
                if (candidatePage.getRowCount() < this.maxRowsPerPage) {
                    return candidatePage;
                }
            }
        }
        Page newPage = this.pager.newPage();
        newPage.setPageType(PageType.DATA_PAGE.value);
        return newPage;
    }

}
