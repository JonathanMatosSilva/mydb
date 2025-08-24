package br.com.mydb;

import java.io.IOException;
import java.util.List;

public class Table {

    private final Pager pager;
    private int rootPageNumber;
    private int firstDataPageNumber;
    private final List<Column> schema;

    public static final int BTREE_MIN_DEGREE = 3;
    public static final int MAX_KEYS_PER_NODE = 2 * BTREE_MIN_DEGREE - 1;
    public static final int MIN_KEYS_PER_NODE = BTREE_MIN_DEGREE - 1;

    public Table(Pager pager, int rootPageNumber, int firstDataPageNumber, List<Column> schema) {
        this.pager = pager;
        this.rootPageNumber = rootPageNumber;
        this.firstDataPageNumber = firstDataPageNumber;
        this.schema = schema;
    }

    public void insert(int keyToInsert, Row rowData) throws IOException {
        byte[] serializedRow = RowSerializer.serialize(rowData, this.schema);
        long dataPointer = writeRecordAndGetDataPointer(serializedRow);

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

        insertIntoSubtree(this.rootPageNumber, keyToInsert, dataPointer);
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

    public Row find(int key) throws IOException {
        byte[] recordBytes = findRaw(key);
        if (recordBytes == null) {
            return null;
        }
        return RowSerializer.deserialize(recordBytes, this.schema);
    }

    public byte[] findRaw(int key) throws IOException {
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

                long dataPointer = node.getDataPointer(mid);

                int dataPageNumber = (int) (dataPointer >> 32);
                int dataSlotId = (int) (dataPointer);
                Page dataPage = this.pager.getPage(dataPageNumber);

                return dataPage.getRecord(dataSlotId);

            } else if (midKey < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return null;
    }

    public Cursor start() throws IOException {
        int firstLeafPageNum = findFirstLeafPageNumber();
        return new Cursor(this, firstLeafPageNum);
    }

    private int findFirstLeafPageNumber() throws IOException {
        int currentPageNum = this.rootPageNumber;
        Page currentPage = this.pager.getPage(currentPageNum);
        BTreeNode node = new BTreeNode(currentPage, BTREE_MIN_DEGREE);

        if (node.isLeaf()) {
            return currentPageNum;
        }

        while (!node.isLeaf()) {
            currentPageNum = node.getChildPointer(0);
            currentPage = this.pager.getPage(currentPageNum);
            node = new BTreeNode(currentPage, BTREE_MIN_DEGREE);
        }

        return currentPageNum;
    }


    public Page getPage(int pageNumber) throws IOException {
        return this.pager.getPage(pageNumber);
    }

    public int getNumPages() {
        return this.pager.getNumPages();
    }

    private long writeRecordAndGetDataPointer(byte[] recordData) throws IOException {
        Page dataPage = findDataPageWithSpace(recordData.length);

        long pageNumber = dataPage.getPageNumber();
        int slotId = dataPage.addRecord(recordData);

        return (pageNumber << 32) | ((long) slotId & 0xFFFFFFFFL);
    }

    private Page findDataPageWithSpace(int requiredSpace) throws IOException {
        int currentPageNum = this.firstDataPageNumber;
        Page lastPageInChain = null;

        while (currentPageNum != BTreeNode.NULL_POINTER) {
            Page currentPage = pager.getPage(currentPageNum);
            lastPageInChain = currentPage;

            if (currentPage.getFreeSpace() >= requiredSpace) {
                return currentPage;
            }
            currentPageNum = currentPage.getNextDataPagePointer();
        }

        Page newPage = pager.newPage();
        newPage.initializeAsDataPage();

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

    public void delete(int keyToDelete) throws IOException {

        long dataPointer = findDataOffset(keyToDelete);
        if (dataPointer == -1L) {
            System.out.println("Chave " + keyToDelete + " não encontrada para exclusão.");
            return;
        }

        deleteFromNode(this.rootPageNumber, keyToDelete);

        Page rootPage = pager.getPage(this.rootPageNumber);
        BTreeNode rootNode = new BTreeNode(rootPage, BTREE_MIN_DEGREE);

        if (!rootNode.isLeaf() && rootNode.getKeyCount() == 0) {
            System.out.println("RAIZ ANTIGA FICOU VAZIA, ATUALIZANDO PARA NOVA RAIZ...");
            this.rootPageNumber = rootNode.getChildPointer(0);
        }

        int dataPageNumber = (int) (dataPointer >> 32);
        int dataSlotId = (int) (dataPointer);

        Page dataPage = this.pager.getPage(dataPageNumber);
        dataPage.deleteRecord(dataSlotId);

        System.out.println("Chave " + keyToDelete + " deletada.");
        printTree();
    }

    private void deleteFromNode(int pageNumber, int keyToDelete) throws IOException {
        Page page = pager.getPage(pageNumber);
        BTreeNode node = new BTreeNode(page, BTREE_MIN_DEGREE);

        if (node.isLeaf()) {
            deleteFromLeaf(node, keyToDelete);
            this.pager.flushPage(page);
            return;
        }

        int childIndex = findNextChildIndex(node, keyToDelete);

        Page childPage = pager.getPage(node.getChildPointer(childIndex));
        BTreeNode childNode = new BTreeNode(childPage, BTREE_MIN_DEGREE);

        if (childNode.getKeyCount() == MIN_KEYS_PER_NODE) {
            ensureSufficientKeys(node, childIndex);
            childIndex = findNextChildIndex(node, keyToDelete);
        }

        deleteFromNode(node.getChildPointer(childIndex), keyToDelete);
    }

    private void ensureSufficientKeys(BTreeNode parentNode, int childIndex) throws IOException {
        Page childPage = pager.getPage(parentNode.getChildPointer(childIndex));
        BTreeNode childNode = new BTreeNode(childPage, BTREE_MIN_DEGREE);

        if (childIndex > 0) {
            Page leftPage = pager.getPage(parentNode.getChildPointer(childIndex - 1));
            BTreeNode leftSibling = new BTreeNode(leftPage, BTREE_MIN_DEGREE);

            if (leftSibling.getKeyCount() > BTREE_MIN_DEGREE - 1) {
                borrowFromLeftSibling(parentNode, childIndex, childNode, leftSibling);
                return;
            }
        }

        if (childIndex < parentNode.getKeyCount()) {
            Page rightPage = pager.getPage(parentNode.getChildPointer(childIndex + 1));
            BTreeNode rightSibling = new BTreeNode(rightPage, BTREE_MIN_DEGREE);

            if (rightSibling.getKeyCount() > BTREE_MIN_DEGREE - 1) {
                borrowFromRightSibling(parentNode, childIndex, childNode, rightSibling);
                return;
            }
        }

        if (childIndex > 0) {
            mergeNodes(parentNode, childIndex);
        } else {
            mergeNodes(parentNode, childIndex + 1);
        }
    }

    private void deleteFromLeaf(BTreeNode node, int keyToDelete) {
        int index = 0;
        while (index < node.getKeyCount() && keyToDelete > node.getKey(index)) {
            index++;
        }

        if (index < node.getKeyCount() && node.getKey(index) == keyToDelete) {
            for (int i = index + 1; i < node.getKeyCount(); i++) {
                node.setKey(i - 1, node.getKey(i));
                node.setDataPointer(i - 1, node.getDataPointer(i));
            }
            node.setKeyCount(node.getKeyCount() - 1);

        }
    }

    private void borrowFromLeftSibling(BTreeNode parentNode, int childIndexOfDeficitNode, BTreeNode nodeWithDeficit, BTreeNode leftSibling) throws IOException {
        if (nodeWithDeficit.isLeaf()) {

            int siblingKeyCount = leftSibling.getKeyCount();
            int keyToMove = leftSibling.getKey(siblingKeyCount - 1);
            long dataPointerToMove = leftSibling.getDataPointer(siblingKeyCount - 1);

            leftSibling.setKeyCount(siblingKeyCount - 1);

            for (int i = nodeWithDeficit.getKeyCount(); i > 0; i--) {
                nodeWithDeficit.setKey(i, nodeWithDeficit.getKey(i - 1));
                nodeWithDeficit.setDataPointer(i, nodeWithDeficit.getDataPointer(i - 1));
            }

            nodeWithDeficit.setKey(0, keyToMove);
            nodeWithDeficit.setDataPointer(0, dataPointerToMove);
            nodeWithDeficit.setKeyCount(nodeWithDeficit.getKeyCount() + 1);

            int parentKeyIndex = childIndexOfDeficitNode - 1;
            parentNode.setKey(parentKeyIndex, keyToMove);

        } else {

            for (int i = nodeWithDeficit.getKeyCount() - 1; i >= 0; i--) {
                nodeWithDeficit.setKey(i + 1, nodeWithDeficit.getKey(i));
            }
            for (int i = nodeWithDeficit.getKeyCount(); i >= 0; i--) {
                nodeWithDeficit.setChildPointer(i + 1, nodeWithDeficit.getChildPointer(i));
            }

            int siblingKeyCount = leftSibling.getKeyCount();
            int parentKeyIndex = childIndexOfDeficitNode - 1;

            nodeWithDeficit.setKey(0, parentNode.getKey(parentKeyIndex));
            nodeWithDeficit.setKeyCount(nodeWithDeficit.getKeyCount() + 1);

            int keyToMove = leftSibling.getKey(siblingKeyCount - 1);
            leftSibling.setKeyCount(siblingKeyCount - 1);

            int childPointerToMove = leftSibling.getChildPointer(siblingKeyCount);

            parentNode.setKey(parentKeyIndex, keyToMove);
            nodeWithDeficit.setChildPointer(0, childPointerToMove);

        }

        this.pager.flushPage(parentNode.getPage());
        this.pager.flushPage(nodeWithDeficit.getPage());
        this.pager.flushPage(leftSibling.getPage());
    }

    private void borrowFromRightSibling(BTreeNode parentNode, int childIndexOfDeficitNode, BTreeNode nodeWithDeficit, BTreeNode rightSibling) throws IOException {
        if (nodeWithDeficit.isLeaf()) {

            int keyToMove = rightSibling.getKey(0);
            long dataPointerToMove = rightSibling.getDataPointer(0);

            int nodeWithDeficitKeyCount = nodeWithDeficit.getKeyCount();
            nodeWithDeficit.setKey(nodeWithDeficitKeyCount, keyToMove);
            nodeWithDeficit.setDataPointer(nodeWithDeficitKeyCount, dataPointerToMove);
            nodeWithDeficit.setKeyCount(nodeWithDeficitKeyCount + 1);

            int siblingKeyCount = rightSibling.getKeyCount();
            for (int i = 0; i < siblingKeyCount - 1; i++) {
                rightSibling.setKey(i, rightSibling.getKey(i + 1));
                rightSibling.setDataPointer(i, rightSibling.getDataPointer(i + 1));
            }
            rightSibling.setKeyCount(siblingKeyCount - 1);

            parentNode.setKey(childIndexOfDeficitNode, rightSibling.getKey(0));

        } else {

            int siblingKeyCount = rightSibling.getKeyCount();

            int nodeWithDeficitKeyCount = nodeWithDeficit.getKeyCount();
            nodeWithDeficit.setKey(nodeWithDeficitKeyCount, parentNode.getKey(childIndexOfDeficitNode));
            nodeWithDeficit.setKeyCount(nodeWithDeficitKeyCount + 1);

            int keyToMove = rightSibling.getKey(0);
            int childPointerToMove = rightSibling.getChildPointer(0);

            for (int i = 0; i < siblingKeyCount - 1; i++) {
                rightSibling.setKey(i, rightSibling.getKey(i + 1));
            }
            for (int i = 0; i < siblingKeyCount; i++) {
                rightSibling.setChildPointer(i, rightSibling.getChildPointer(i + 1));
            }
            rightSibling.setKeyCount(siblingKeyCount - 1);

            parentNode.setKey(childIndexOfDeficitNode, keyToMove);
            nodeWithDeficit.setChildPointer(nodeWithDeficit.getKeyCount(), childPointerToMove);

        }

        this.pager.flushPage(parentNode.getPage());
        this.pager.flushPage(nodeWithDeficit.getPage());
        this.pager.flushPage(rightSibling.getPage());
    }

    private void mergeNodes(BTreeNode parentNode, int rightNodeIndex) throws IOException {
        int leftNodeIndex = rightNodeIndex - 1;

        BTreeNode leftNode = new BTreeNode(pager.getPage(parentNode.getChildPointer(leftNodeIndex)), BTREE_MIN_DEGREE);
        BTreeNode rightNode = new BTreeNode(pager.getPage(parentNode.getChildPointer(rightNodeIndex)), BTREE_MIN_DEGREE);

        if (rightNode.isLeaf()) {

            int leftNodeKeyCount = leftNode.getKeyCount();
            int rightNodeKeyCount = rightNode.getKeyCount();

            for (int i = 0; i < rightNodeKeyCount; i++) {
                leftNode.setKey(leftNodeKeyCount + i, rightNode.getKey(i));
                leftNode.setDataPointer(leftNodeKeyCount + i, rightNode.getDataPointer(i));
            }
            leftNode.setKeyCount(leftNodeKeyCount + rightNodeKeyCount);
            leftNode.setNextSiblingPointer(rightNode.getNextSiblingPointer());

        } else {

            int rightNodeKeyCount = rightNode.getKeyCount();
            int leftNodeKeyCount = leftNode.getKeyCount();

            int parentKeyToMove = parentNode.getKey(rightNodeIndex - 1);
            leftNode.setKey(leftNode.getKeyCount(), parentKeyToMove);
            leftNodeKeyCount += 1;

            for (int i = 0; i < rightNodeKeyCount; i++) {
                leftNode.setKey(leftNodeKeyCount + i, rightNode.getKey(i));
            }
            for (int i = 0; i <= rightNodeKeyCount; i++) {
                leftNode.setChildPointer(leftNodeKeyCount + i, rightNode.getChildPointer(i));
            }

            leftNode.setKeyCount(leftNodeKeyCount + rightNode.getKeyCount());
        }

        int parentKeyToRemoveIndex = rightNodeIndex - 1;
        int parentNodeKeyCount = parentNode.getKeyCount();
        for (int i = parentKeyToRemoveIndex; i < parentNodeKeyCount - 1; i++) {
            parentNode.setKey(i, parentNode.getKey(i + 1));
        }
        for (int i = rightNodeIndex; i < parentNodeKeyCount; i++) {
            parentNode.setChildPointer(i, parentNode.getChildPointer(i + 1));
        }
        parentNode.setKeyCount(parentNodeKeyCount - 1);

        this.pager.flushPage(parentNode.getPage());
        this.pager.flushPage(leftNode.getPage());
    }

    public boolean update(int keyToUpdate, Row newRowData) throws IOException {
        long dataPointer = findDataOffset(keyToUpdate);

        if (dataPointer == -1L) {
            System.out.println("Chave " + keyToUpdate + " não encontrada para atualização.");
            return false;
        }

        delete(keyToUpdate);
        insert(keyToUpdate, newRowData);

        System.out.println("Chave " + keyToUpdate + " atualizada com sucesso.");
        return true;
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

    public List<Column> getSchema() {
        return schema;
    }
}
