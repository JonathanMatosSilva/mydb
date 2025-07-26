package br.com.mydb;

import java.io.IOException;

public class Table {

    private final Pager pager;
    private int rootPageNumber;

    private final int maxRowsPerPage;

    public static final int BTREE_MIN_DEGREE = 3;
    public static final int MAX_KEYS_PER_NODE = 2 * BTREE_MIN_DEGREE - 1;

    public Table(Pager pager, int rootPageNumber) {
        this.pager = pager;
        this.rootPageNumber = rootPageNumber;
        this.maxRowsPerPage = (pager.getPageSize() - Page.HEADER_SIZE) / User.ROW_SIZE;
//        this.maxLeafCellsPerPage = (pager.getPageSize() - Page.HEADER_SIZE) / BTreeNode.CELL_SIZE;
    }

    public void insert(User user) throws IOException {

        long dataOffset = writeDataRowAndGetOffset(user);
        int keyToInsert = user.getId();

        // insere btree

        Page oldRootPage = pager.getPage(this.rootPageNumber);
        BTreeNode oldRootNode = new BTreeNode(oldRootPage, BTREE_MIN_DEGREE);

        if (oldRootNode.getKeyCount() == MAX_KEYS_PER_NODE) {
            splitRootAndInsert(oldRootNode, keyToInsert, dataOffset);
        } else {
            insertIntoLeaf(oldRootNode, keyToInsert, dataOffset);
        }
        printTree();
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

    private long writeDataRowAndGetOffset(User user) throws IOException {
        byte[] rowBytes = user.toBytes();
        long dataOffset;

        Page dataPage = findDataPageWithSpace();

        int slotData = dataPage.getRowCount();
        dataPage.setRow(slotData, rowBytes);
        dataPage.setRowCount(slotData + 1);

        long offsetInPage =  Page.HEADER_SIZE + ((long) slotData * User.ROW_SIZE);
        dataOffset = ((long) dataPage.getPageNumber() * this.pager.getPageSize()) + offsetInPage;

        this.pager.flushPage(dataPage);

        return dataOffset;
    }

    private void insertIntoLeaf(BTreeNode node, int key, long dataOffset) throws IOException {
        int currentKeyCount = node.getKeyCount();

        int slotToInsert = 0;
        while (slotToInsert < currentKeyCount && node.getKey(slotToInsert) < key) {
            slotToInsert++;
        }

        for (int i = currentKeyCount; i > slotToInsert; i--) {
            node.setKey(i, node.getKey(i - 1));
            node.setDataPointer(i, node.getDataPointer(i - 1));
        }

        node.setKey(slotToInsert, key);
        node.setDataPointer(slotToInsert, dataOffset);
        node.setKeyCount(currentKeyCount + 1);
    }

    private void splitRootAndInsert(BTreeNode oldRootLeaf, int newKey, long newDataPointer) throws IOException {

        Page rightPage = this.pager.newPage();
        rightPage.setPageType(PageType.BTREE_LEAF_NODE.value);
        BTreeNode rightNode = new BTreeNode(rightPage, BTREE_MIN_DEGREE);

        int middleIndex = BTREE_MIN_DEGREE - 1;

        int j = 0;
        for (int i = middleIndex; i < oldRootLeaf.getKeyCount(); i++) {
            rightNode.setKey(j, oldRootLeaf.getKey(i));
            rightNode.setDataPointer(j, oldRootLeaf.getDataPointer(i));
            j++;
        }

        oldRootLeaf.setKeyCount(middleIndex);
        rightNode.setKeyCount(j);

        int promotedKey = rightNode.getKey(0);

        if (newKey >= promotedKey) {
            insertIntoLeaf(rightNode, newKey, newDataPointer);
        } else {
            insertIntoLeaf(oldRootLeaf, newKey, newDataPointer);
        }

        rightNode.setNextSiblingPointer(-1);
        oldRootLeaf.setNextSiblingPointer(rightNode.getPageNumber());

        Page newRootPage = this.pager.newPage();
        newRootPage.setPageType(PageType.BTREE_INTERNAL_NODE.value);
        BTreeNode newRootNode = new BTreeNode(newRootPage, BTREE_MIN_DEGREE);

        newRootNode.setChildPointer(0, oldRootLeaf.getPageNumber());
        newRootNode.setKey(0, promotedKey);
        newRootNode.setChildPointer(1, rightNode.getPageNumber());
        newRootNode.setKeyCount(1);

        this.pager.flushPage(oldRootLeaf.getPage());
        this.pager.flushPage(rightNode.getPage());
        this.pager.flushPage(newRootNode.getPage());

        this.rootPageNumber = newRootNode.getPageNumber();
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

    public void printTree() throws IOException {
        System.out.println("--- ESTRUTURA DA B+ TREE ---");
        if (this.pager.getNumPages() == 0) {
            System.out.println("Árvore está vazia.");
            return;
        }
        printNode(this.rootPageNumber, "");
        System.out.println("--------------------------");
    }


    private void printNode(int pageNumber, String indent) throws IOException {

        Page page = pager.getPage(pageNumber);
        BTreeNode node = new BTreeNode(page, BTREE_MIN_DEGREE);

        if (node.isLeaf()) {
            System.out.print(indent + "Folha (Pág " + pageNumber + ") chaves=[");
            for (int i = 0; i < node.getKeyCount(); i++) {
                System.out.print(node.getKey(i) + (i == node.getKeyCount() - 1 ? "" : ", "));
            }
            System.out.println("] -> (próx: " + node.getNextSiblingPointer() + ")");
        } else {
            System.out.print(indent + "Interno (Pág " + pageNumber + ") chaves=[");
            for (int i = 0; i < node.getKeyCount(); i++) {
                System.out.print(node.getKey(i) + (i == node.getKeyCount() - 1 ? "" : ", "));
            }
            System.out.println("]");

            for (int i = 0; i <= node.getKeyCount(); i++) {
                int childPageNumber = node.getChildPointer(i);
                printNode(childPageNumber, indent + "  ");
            }
        }
    }

}
