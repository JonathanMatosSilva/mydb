package br.com.mydb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class Database {

    private final Pager pager;
    private final Table catalogTable;

    private static final int ROOT_PAGE_POINTER_OFFSET = 0;
    private static final int CATALOG_ROOT_PAGE = 1;
    private static final int CATALOG_FIRST_DATA_PAGE = 2;

    public Database(String databaseFilePath) throws IOException {
        this.pager = new Pager(databaseFilePath, 4096);
        int catalogRootNum;
        int catalogFirstDataPageNum;

        if (pager.getNumPages() == 0) {
            initializeNewDatabase();
            catalogRootNum = CATALOG_ROOT_PAGE;
            catalogFirstDataPageNum = CATALOG_FIRST_DATA_PAGE;
        } else {
            catalogRootNum = readRootPageNumberFromHeader();
            catalogFirstDataPageNum = CATALOG_FIRST_DATA_PAGE;
        }

        this.catalogTable = new Table(pager, catalogRootNum, catalogFirstDataPageNum, CatalogRecord.ROW_SIZE);
    }

    public Table openTable(String tableName) throws IOException {
        int key = tableName.hashCode();
        byte[] recordBytes = catalogTable.find(key);

        if (recordBytes == null) {
            throw new IOException("Tabela '" + tableName + "' não encontrada.");
        }

        CatalogRecord record = CatalogRecord.fromBytes(recordBytes);
        System.out.println("record lidos " + record.toString() + " tamanho: " + recordBytes.length);
        return new Table(this.pager, record.getRootPageNumber(), record.getFirstDataPageNumber(), record.getRowSize());
    }

    private void initializeNewDatabase() throws IOException {
        if (pager.getNumPages() != 0) {
            throw new IllegalStateException("A inicialização só pode ocorrer em um banco de dados vazio.");
        }

        // Página 0: Cabeçalho
        Page headerPage = pager.newPage();
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerPage.getBytes());
        headerBuffer.putInt(ROOT_PAGE_POINTER_OFFSET, CATALOG_ROOT_PAGE);
        headerPage.markAsDirty();

        // Página 1: Raiz da B-Tree do Catálogo
        Page rootCatalogPage = pager.newPage();
        rootCatalogPage.setPageType(PageType.BTREE_LEAF_NODE.value);
        BTreeNode rootNode = new BTreeNode(rootCatalogPage, Table.BTREE_MIN_DEGREE);
        rootNode.setKeyCount(0);
        rootNode.setNextSiblingPointer(BTreeNode.NULL_POINTER);

        // Página 2: Primeira Página de Dados do Catálogo
        Page firstDataPageForCatalog = pager.newPage();
        firstDataPageForCatalog.setPageType(PageType.DATA_PAGE.value);
        firstDataPageForCatalog.setRowCount(0);
        firstDataPageForCatalog.setNextDataPagePointer(BTreeNode.NULL_POINTER);

        // Salva todas as páginas criadas
        pager.flushPage(headerPage);
        pager.flushPage(rootCatalogPage);
        pager.flushPage(firstDataPageForCatalog);
    }

    public void createTable(String tableName, int rowSize) throws IOException {
        Page newTableRootPage = pager.newPage();
        newTableRootPage.setPageType(PageType.BTREE_LEAF_NODE.value);
        BTreeNode rootNode = new BTreeNode(newTableRootPage, Table.BTREE_MIN_DEGREE);
        rootNode.setKeyCount(0);
        rootNode.setNextSiblingPointer(BTreeNode.NULL_POINTER);
        pager.flushPage(newTableRootPage);
        int newTableRootPageNum = newTableRootPage.getPageNumber();

        Page firstDataPage = pager.newPage();
        firstDataPage.setPageType(PageType.DATA_PAGE.value);
        firstDataPage.setRowCount(0);
        firstDataPage.setNextDataPagePointer(BTreeNode.NULL_POINTER);
        pager.flushPage(firstDataPage);
        int firstDataPageNum = firstDataPage.getPageNumber();

        CatalogRecord record = new CatalogRecord(tableName, newTableRootPageNum, firstDataPageNum, rowSize);
        System.out.println("bytes escritos " + Arrays.toString(record.toBytes()));
        catalogTable.insert(record.getTableName().hashCode(), record.toBytes());
    }

    private int readRootPageNumberFromHeader() throws IOException {
        Page headerPage = pager.getPage(0);
        ByteBuffer buffer = ByteBuffer.wrap(headerPage.getBytes());
        return buffer.getInt(ROOT_PAGE_POINTER_OFFSET);
    }

    public void close(Map<String, Table> openTables) throws IOException {
        for (Map.Entry<String, Table> entry : openTables.entrySet()) {
            String tableName = entry.getKey();
            Table table = entry.getValue();

            System.out.println("Preparando para salvar estado da tabela '" + tableName + "'...");

            long offset = this.catalogTable.findDataOffset(tableName.hashCode());

            if (offset != -1L) {
                int pageNumber = (int) (offset / pager.getPageSize());
                int offsetInPage = (int) (offset % pager.getPageSize());

                Page catalogDataPage = pager.getPage(pageNumber);

                int finalRootPageNum = table.getRootPageNumber();
                int firstDataPageNum = table.getFirstDataPageNumber();
                int rowSize = table.getRowSize();
                CatalogRecord updatedRecord = new CatalogRecord(tableName, finalRootPageNum, firstDataPageNum, rowSize);
                byte[] updatedBytes = updatedRecord.toBytes();

                System.arraycopy(updatedBytes, 0, catalogDataPage.getBytes(), offsetInPage, updatedBytes.length);
                catalogDataPage.markAsDirty();

                System.out.println("-> Estado salvo em memória. Nova raiz: " + finalRootPageNum);
            }
        }
        this.pager.close();
    }
}
