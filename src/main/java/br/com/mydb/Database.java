package br.com.mydb;

import java.io.IOException;

public class Database {
    private Pager pager;

    public Database(String databaseFilePath) throws IOException {
        this.pager = new Pager(databaseFilePath, 4096);
    }

    public Table openTable() throws IOException {
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
