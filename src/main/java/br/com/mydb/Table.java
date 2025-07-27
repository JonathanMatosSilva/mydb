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
    }

    public void insert(User user) throws IOException {
        long dataOffset = writeDataRowAndGetOffset(user);
        int keyToInsert = user.getId();

        Page rootPage = this.pager.getPage(this.rootPageNumber);
        BTreeNode rootNode = new BTreeNode(rootPage, BTREE_MIN_DEGREE);

        if (rootNode.getKeyCount() == MAX_KEYS_PER_NODE) {

            Page newRootPage = this.pager.newPage();
            newRootPage.setPageType(PageType.BTREE_INTERNAL_NODE.value);
            BTreeNode newRoot = new BTreeNode(newRootPage, BTREE_MIN_DEGREE);

            newRoot.setChildPointer(0, rootNode.getPageNumber());

            PromotedKey promotedKey;
            if (rootNode.isLeaf()) {
                promotedKey = splitLeafNode(rootNode);
            } else {
                promotedKey = splitInternalNode(rootNode);
            }

            newRoot.setKey(0, promotedKey.getKey());
            newRoot.setChildPointer(1, promotedKey.getRightChildPageNumber());
            newRoot.setKeyCount(1);

            this.rootPageNumber = newRoot.getPageNumber();
            pager.flushPage(newRoot.getPage());
        }

        insertIntoSubtree(this.rootPageNumber, keyToInsert, dataOffset);
        printTree();
    }

    private void insertIntoSubtree(int pageNumber, int key, long dataOffset) throws IOException {
        Page currentPage = this.pager.getPage(pageNumber);
        BTreeNode node = new BTreeNode(currentPage, BTREE_MIN_DEGREE);

        if (node.isLeaf()) {
            insertIntoLeaf(node, key, dataOffset);
            this.pager.flushPage(currentPage);
            return;
        }

        int childIndex = findNextChildIndex(node, key);
        int childPageNumber = node.getChildPointer(childIndex);

        Page childPage = this.pager.getPage(childPageNumber);
        BTreeNode childNode = new BTreeNode(childPage, BTREE_MIN_DEGREE);

        if (childNode.getKeyCount() == MAX_KEYS_PER_NODE) {
            System.out.println("Split no interno");
            splitChildNode(node, childIndex, childNode);

            if (key > node.getKey(childIndex)) {
                childPageNumber = node.getChildPointer(childIndex + 1);
            }
        }

        insertIntoSubtree(childPageNumber, key, dataOffset);
    }

    private int findNextChildIndex(BTreeNode node, int key) {
        int nextChildIndex = 0;
        while (nextChildIndex < node.getKeyCount() && node.getKey(nextChildIndex) <= key) {
            nextChildIndex++;
        }
        return nextChildIndex;
    }

    private void splitChildNode(BTreeNode parentNode, int childIndex, BTreeNode childToSplit) throws IOException {

        PromotedKey promotedKey;

        if (childToSplit.isLeaf()) {
            promotedKey = splitLeafNode(childToSplit);
        } else {
            promotedKey = splitInternalNode(childToSplit);
        }

        insertIntoInternalNode(parentNode, childIndex, promotedKey);

        this.pager.flushPage(parentNode.getPage());
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

    private PromotedKey splitLeafNode(BTreeNode nodeToSplit) throws IOException {

        Page rightPage = this.pager.newPage();
        rightPage.setPageType(PageType.BTREE_LEAF_NODE.value);
        BTreeNode rightNode = new BTreeNode(rightPage, BTREE_MIN_DEGREE);

        int middleIndex = BTREE_MIN_DEGREE - 1;

        int j = 0;
        for (int i = middleIndex; i < nodeToSplit.getKeyCount(); i++) {
            rightNode.setKey(j, nodeToSplit.getKey(i));
            rightNode.setDataPointer(j, nodeToSplit.getDataPointer(i));
            j++;
        }

        nodeToSplit.setKeyCount(middleIndex);
        rightNode.setKeyCount(j);

        PromotedKey promotedKey = new PromotedKey(rightNode.getKey(0), rightNode.getPageNumber());

        return promotedKey;
    }

    /**
     * (Futuro) Divide um nó interno cheio. A lógica é similar à de um nó folha,
     * mas lida com ponteiros para filhos em vez de ponteiros para dados.
     */
    private PromotedKey splitInternalNode(BTreeNode nodeToSplit) throws IOException {
        throw new UnsupportedOperationException("Split de nó interno ainda não implementado.");
    }

    private void insertIntoInternalNode(BTreeNode parentNode, int index, PromotedKey promotedKey) {
        for (int i = parentNode.getKeyCount(); i > index; i--) {
            parentNode.setKey(i, parentNode.getKey(i - 1));
            parentNode.setChildPointer(i + 1, parentNode.getChildPointer(i));
        }

        parentNode.setKey(index, promotedKey.getKey());
        parentNode.setChildPointer(index + 1, promotedKey.getRightChildPageNumber());

        parentNode.setKeyCount(parentNode.getKeyCount() + 1);
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
