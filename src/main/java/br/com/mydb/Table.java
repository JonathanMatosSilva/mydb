package br.com.mydb;

import java.io.IOException;

public class Table {

    private final Pager pager;
    private int rootPageNumber;

    private final int maxRowsPerPage;

    public Table(Pager pager, int rootPageNumber) {
        this.pager = pager;
        this.rootPageNumber = rootPageNumber;
        this.maxRowsPerPage = (pager.getPageSize() - Page.HEADER_SIZE) / User.ROW_SIZE;
    }

    public void insert(User user) throws IOException {
        byte[] rowBytes = user.toBytes();

        int lastPageNum = pager.getNumPages() - 1;
        Page page;

        if (lastPageNum >= 0) {
            page = pager.getPage(lastPageNum);

            if (page.getRowCount() < maxRowsPerPage){
                int slot = page.getRowCount();
                page.setRow(slot, rowBytes);
                page.setRowCount(slot + 1);
                pager.flushPage(page);
                return;
            }
        }

        page = pager.newPage();
        page.setPageType(PageType.DATA_PAGE.value);
        page.setRow(0, rowBytes);
        page.setRowCount(1);
        pager.flushPage(page);
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

}
