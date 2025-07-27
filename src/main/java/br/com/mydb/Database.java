package br.com.mydb;

import java.io.IOException;
import java.nio.ByteBuffer;

public class Database {

    private static final int ROOT_PAGE_POINTER_OFFSET = 0;

    private final Pager pager;

    public Database(String databaseFilePath) throws IOException {
        this.pager = new Pager(databaseFilePath, 4096);
    }

    public Table openTable() throws IOException {
        int rootPageNum;

        if (pager.getNumPages() == 0) {
            rootPageNum = initializeNewDatabase();
        } else {
            rootPageNum = readRootPageNumberFromHeader();
        }
        return new Table(this.pager, rootPageNum);
    }

    private int initializeNewDatabase() throws IOException {
        if (pager.getNumPages() != 0) {
            throw new IllegalStateException("A inicialização só pode ocorrer em um banco de dados vazio.");
        }

        Page headerPage = pager.newPage();
        if (headerPage.getPageNumber() != 0) {
            throw new IllegalStateException("A primeira página criada não é a página 0.");
        }

        Page rootDataPage = pager.newPage();
        if (rootDataPage.getPageNumber() != 1) {
            throw new IllegalStateException("A segunda página criada não é a página 1.");
        }

        rootDataPage.setPageType(PageType.BTREE_LEAF_NODE.value);
        BTreeNode rootNode = new BTreeNode(rootDataPage, Table.BTREE_MIN_DEGREE);
        rootNode.setKeyCount(0);
        rootNode.setNextSiblingPointer(BTreeNode.NULL_POINTER);

        int rootPageNumber = rootDataPage.getPageNumber();

        ByteBuffer headerBuffer = ByteBuffer.wrap(headerPage.getBytes());
        headerBuffer.putInt(ROOT_PAGE_POINTER_OFFSET, rootPageNumber);
        headerPage.markAsDirty();

        pager.flushPage(headerPage);
        pager.flushPage(rootDataPage);

        return rootPageNumber;
    }

    private int readRootPageNumberFromHeader() throws IOException {
        Page headerPage = pager.getPage(0);
        ByteBuffer buffer = ByteBuffer.wrap(headerPage.getBytes());
        return buffer.getInt(ROOT_PAGE_POINTER_OFFSET);
    }

    public void close(Table table) throws IOException {
        int finalRootPageNumber = table.getRootPageNumber();

        Page headerPage = this.pager.getPage(0);

        ByteBuffer headerBuffer = ByteBuffer.wrap(headerPage.getBytes());
        headerBuffer.putInt(ROOT_PAGE_POINTER_OFFSET, finalRootPageNumber);
        headerPage.markAsDirty();

        this.pager.flushPage(headerPage);

        this.pager.close();
    }
}
