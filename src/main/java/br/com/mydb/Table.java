package br.com.mydb;

import java.io.IOException;
import java.util.Arrays;

public class Table {

    private final Pager pager;
    private int rootPageNumber;
    private int firstDataPageNumber;
    private final int rowSize;

    private final int maxRowsPerPage;

    public static final int BTREE_MIN_DEGREE = 3;
    public static final int MAX_KEYS_PER_NODE = 2 * BTREE_MIN_DEGREE - 1;

    public Table(Pager pager, int rootPageNumber, int firstDataPageNumber, int rowSize) {
        this.pager = pager;
        this.rootPageNumber = rootPageNumber;
        this.firstDataPageNumber = firstDataPageNumber;
        this.rowSize = rowSize;
        if (rowSize > 0) {
            this.maxRowsPerPage = (pager.getPageSize() - Page.HEADER_SIZE) / rowSize;
        } else {
            this.maxRowsPerPage = 0;
        }
    }

    public void insert(int keyToInsert, byte[] rowData) throws IOException {
        System.out.println("DEBUG: Table.insert chamado. Chave: " + keyToInsert + ". Tamanho da linha desta tabela: " + this.rowSize);
        long dataOffset = writeDataRowAndGetOffset(rowData);

        Page rootPage = this.pager.getPage(this.rootPageNumber);
        BTreeNode rootNode = new BTreeNode(rootPage, BTREE_MIN_DEGREE);

        if (rootNode.getKeyCount() == MAX_KEYS_PER_NODE) {

            Page newRootPage = this.pager.newPage();
            newRootPage.setPageType(PageType.BTREE_INTERNAL_NODE.value);
            BTreeNode newRoot = new BTreeNode(newRootPage, BTREE_MIN_DEGREE);

            PromotedKey promotedKey;
            if (rootNode.isLeaf()) {
                promotedKey = splitLeafNode(rootNode);
            } else {
                promotedKey = splitInternalNode(rootNode);
            }

            newRoot.setKey(0, promotedKey.getKey());
            newRoot.setChildPointer(0, rootNode.getPageNumber());
            newRoot.setChildPointer(1, promotedKey.getRightChildPageNumber());
            newRoot.setKeyCount(1);

            this.rootPageNumber = newRoot.getPageNumber();
            this.pager.flushPage(newRoot.getPage());

        }

        insertIntoSubtree(this.rootPageNumber, keyToInsert, dataOffset);
        printTree();
    }

    private void insertIntoSubtree(int pageNumber, int key, long dataOffset) throws IOException {
        Page currentPage = this.pager.getPage(pageNumber);
        BTreeNode node = new BTreeNode(currentPage, BTREE_MIN_DEGREE);

        if (node.isLeaf()) {
            insertIntoLeaf(node, key, dataOffset);
            return;
        }

        int childIndex = findNextChildIndex(node, key);
        int childPageNumber = node.getChildPointer(childIndex);

        Page childPage = this.pager.getPage(childPageNumber);
        BTreeNode childNode = new BTreeNode(childPage, BTREE_MIN_DEGREE);

        if (childNode.getKeyCount() == MAX_KEYS_PER_NODE) {
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
        System.out.println("DEBUG: [insertIntoLeaf] Inserindo ponteiro " + dataOffset + " para a chave " + key);
        node.setDataPointer(slotToInsert, dataOffset);
        node.setKeyCount(currentKeyCount + 1);
        this.pager.flushPage(node.getPage());
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

        rightNode.setNextSiblingPointer(nodeToSplit.getNextSiblingPointer());
        nodeToSplit.setNextSiblingPointer(rightNode.getPageNumber());

        this.pager.flushPage(nodeToSplit.getPage());
        this.pager.flushPage(rightNode.getPage());

        return promotedKey;
    }

    private PromotedKey splitInternalNode(BTreeNode nodeToSplit) throws IOException {
        Page rightPage = this.pager.newPage();
        rightPage.setPageType(PageType.BTREE_INTERNAL_NODE.value);
        BTreeNode rightNode = new BTreeNode(rightPage, BTREE_MIN_DEGREE);

        int middleIndex = BTREE_MIN_DEGREE - 1;
        int promotedKey = nodeToSplit.getKey(middleIndex);

        int j = 0;
        for (int i = middleIndex + 1; i < nodeToSplit.getKeyCount(); i++) {
            rightNode.setKey(j, nodeToSplit.getKey(i));
            j++;
        }

        j = 0;
        for (int i = middleIndex + 1; i <= nodeToSplit.getKeyCount(); i++) {
            rightNode.setChildPointer(j, nodeToSplit.getChildPointer(i));
            j++;
        }

        nodeToSplit.setKeyCount(middleIndex);
        rightNode.setKeyCount(j - 1);

        this.pager.flushPage(nodeToSplit.getPage());
        this.pager.flushPage(rightNode.getPage());

        return new PromotedKey(promotedKey, rightNode.getPageNumber());
    }

    private void insertIntoInternalNode(BTreeNode parentNode, int index, PromotedKey promotedKey) throws IOException {
        for (int i = parentNode.getKeyCount(); i > index; i--) {
            parentNode.setKey(i, parentNode.getKey(i - 1));
            parentNode.setChildPointer(i + 1, parentNode.getChildPointer(i));
        }

        parentNode.setKey(index, promotedKey.getKey());
        parentNode.setChildPointer(index + 1, promotedKey.getRightChildPageNumber());

        parentNode.setKeyCount(parentNode.getKeyCount() + 1);

        this.pager.flushPage(parentNode.getPage());
    }


    public byte[] find(int key) throws IOException {
        Page rootPage = this.pager.getPage(this.rootPageNumber);
        BTreeNode node = new BTreeNode(rootPage, BTREE_MIN_DEGREE);

        while (!node.isLeaf()) {
            int childIndex = findNextChildIndex(node, key);
            int childPageNumber = node.getChildPointer(childIndex);
            Page childPage = this.pager.getPage(childPageNumber);
            node = new BTreeNode(childPage, BTREE_MIN_DEGREE);
        }

        int left = 0;
        int right = node.getKeyCount() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            int midKey = node.getKey(mid);

            if (midKey == key) {

                long dataOffset = node.getDataPointer(mid);
                int dataPageNumber = (int) (dataOffset / this.pager.getPageSize());
                int offsetInPage = (int) (dataOffset % this.pager.getPageSize());

                Page dataPage = this.pager.getPage(dataPageNumber);
                System.out.println("pagina: " + dataPageNumber + "\noffset: " + offsetInPage + "\nbytes lidos " + Arrays.toString(dataPage.getBytes()));
                byte[] rowData = new byte[this.rowSize];
                System.arraycopy(dataPage.getBytes(), offsetInPage, rowData, 0, this.rowSize);

                return rowData;

            } else if (midKey < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
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

    private long writeDataRowAndGetOffset(byte[] rowBytes) throws IOException {
        long dataOffset;

        Page dataPage = findDataPageWithSpace();

        int slotData = dataPage.getRowCount();
        dataPage.setRow(slotData, rowBytes, this.rowSize);
        dataPage.setRowCount(slotData + 1);

        long offsetInPage =  Page.HEADER_SIZE + ((long) slotData * this.rowSize);
        dataOffset = ((long) dataPage.getPageNumber() * this.pager.getPageSize()) + offsetInPage;

        this.pager.flushPage(dataPage);

        return dataOffset;
    }

    private Page findDataPageWithSpace() throws IOException {
        int currentPageNum = this.firstDataPageNumber;
        Page lastPageInChain = null;

        while (currentPageNum != BTreeNode.NULL_POINTER) {
            Page currentPage = pager.getPage(currentPageNum);
            lastPageInChain = currentPage;

            if (currentPage.getRowCount() < this.maxRowsPerPage) {
                return currentPage;
            }
            currentPageNum = currentPage.getNextDataPagePointer();
        }

        Page newPage = pager.newPage();
        newPage.setPageType(PageType.DATA_PAGE.value);
        newPage.setRowCount(0);
        newPage.setNextDataPagePointer(BTreeNode.NULL_POINTER);

        if (lastPageInChain != null) {
            lastPageInChain.setNextDataPagePointer(newPage.getPageNumber());
            pager.flushPage(lastPageInChain);
        } else {
            this.firstDataPageNumber = newPage.getPageNumber();
        }
        pager.flushPage(newPage);
        return newPage;
    }

    public long findDataOffset(int key) throws IOException {
        Page rootPage = this.pager.getPage(this.rootPageNumber);
        BTreeNode node = new BTreeNode(rootPage, BTREE_MIN_DEGREE);

        while (!node.isLeaf()) {
            int childIndex = findNextChildIndex(node, key);
            int childPageNumber = node.getChildPointer(childIndex);
            Page childPage = this.pager.getPage(childPageNumber);
            node = new BTreeNode(childPage, BTREE_MIN_DEGREE);
        }

        int left = 0;
        int right = node.getKeyCount() - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            int midKey = node.getKey(mid);

            if (midKey == key) {
                return node.getDataPointer(mid);
            } else if (midKey < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return -1L;
    }

    public int getFirstDataPageNumber() {
        return this.firstDataPageNumber;
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

    public int getRootPageNumber() {
        return this.rootPageNumber;
    }

    public Pager getPager() {
        return pager;
    }

    public int getRowSize() {
        return rowSize;
    }
}
