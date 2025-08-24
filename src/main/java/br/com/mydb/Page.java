package br.com.mydb;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

public class Page {

    private static final int PAGE_TYPE_OFFSET = 0; // 1 byte
    private static final int ROW_COUNT_OFFSET = 1; // 2 bytes
    private static final int FREE_SPACE_POINTER_OFFSET = 3; // 2 bytes

    public static final int NEXT_SIBLING_POINTER_OFFSET = 5; // 4 bytes
    public static final int NEXT_DATA_PAGE_POINTER_OFFSET = 9; // 4 bytes

    public static final int HEADER_SIZE =  1 + 2 + 2 + 4 + 4; // 13 bytes

    public static final int SLOT_SIZE = 4;
    public static final int SLOT_OFFSET_FIELD = 0;
    public static final int SLOT_LENGTH_FIELD = 2;

    private final int pageNumber;
    private final byte[] data;
    private boolean isDirty;
    private final ByteBuffer buffer;


    public Page(int pageNumber, byte[] data) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.isDirty = false;
        this.buffer = ByteBuffer.wrap(data);
    }

    public byte getPageType() {
        return this.buffer.get(PAGE_TYPE_OFFSET);
    }

    public void setPageType(byte pageType) {
        this.buffer.put(PAGE_TYPE_OFFSET, pageType);
    }

    public int getRowCount() {
        return this.buffer.getShort(ROW_COUNT_OFFSET );
    }

    public void setRowCount(int count) {
        this.buffer.putShort(ROW_COUNT_OFFSET , (short) count);
    }

    public int getFreeSpacePointer() {
        return this.buffer.getShort(FREE_SPACE_POINTER_OFFSET);
    }

    public void setFreeSpacePointer(int pointer) {
        this.buffer.putShort(FREE_SPACE_POINTER_OFFSET, (short) pointer);
    }

    public int getPageNumber() {
        return this.pageNumber;
    }

    public boolean isDirty() {
        return this.isDirty;
    }

    public void markAsDirty() {
        this.isDirty = true;
    }

    public byte[] getBytes() {
        return this.data;
    }

    public int getNextDataPagePointer() {
        return buffer.getInt(NEXT_DATA_PAGE_POINTER_OFFSET);
    }

    public void setNextDataPagePointer(int pageNumber) {
        this.buffer.putInt(NEXT_DATA_PAGE_POINTER_OFFSET, pageNumber);
    }

    public void initialize() {
        setPageType(PageType.DATA_PAGE.value);
        setRowCount(0);
        setFreeSpacePointer(this.data.length);
        setNextDataPagePointer(BTreeNode.NULL_POINTER);
    }

    public int getFreeSpace() {
        int endOfSlotArray = HEADER_SIZE + (SLOT_SIZE * getRowCount());
        return getFreeSpacePointer() - endOfSlotArray;
    }

    public int addRecord(byte[] recordData) {
        int recordSize = recordData.length;
        if (getFreeSpace() < recordSize + SLOT_SIZE) {
            throw new IllegalStateException("Não há espaço suficiente na página " + this.pageNumber + " para o registro.");
        }

        int newFreeSpacePointer = getFreeSpacePointer() - recordSize;
        setFreeSpacePointer(newFreeSpacePointer);

        System.arraycopy(recordData, 0, this.data, newFreeSpacePointer, recordSize);

        int newSlotId = getRowCount();
        int slotOffset = HEADER_SIZE + (newSlotId * SLOT_SIZE);

        this.buffer.putShort(slotOffset + SLOT_OFFSET_FIELD, (short) newFreeSpacePointer);
        this.buffer.putShort(slotOffset + SLOT_LENGTH_FIELD, (short) recordSize);

        setRowCount(newSlotId + 1);
        markAsDirty();

        return newSlotId;
    }

    public byte[] getRecord(int slotId) throws NoSuchElementException {
        if (slotId < 0 || slotId > getRowCount()) {
            throw new NoSuchElementException("Slot ID " + slotId + " é inválido para a página " + pageNumber);
        }

        int slotOffset = HEADER_SIZE + (slotId * SLOT_SIZE);
        int dataOffset = this.buffer.getShort(slotOffset + SLOT_OFFSET_FIELD);
        int dataLength = this.buffer.getShort(slotOffset + SLOT_LENGTH_FIELD);

        if (dataLength == 0) {
            throw new NoSuchElementException("O registro no slot " + slotId + " foi deletado.");
        }

        byte[] record = new byte[dataLength];
        System.arraycopy(this.data, dataOffset, record, 0, dataLength);

        return record;
    }

    public void deleteRecord(int slotId) {
        if (slotId < 0 || slotId > getRowCount()) {
            throw new NoSuchElementException("Slot ID " + slotId + " é inválido para a página " + pageNumber);
        }

        int slotOffset = HEADER_SIZE + (slotId * SLOT_SIZE);

        this.buffer.putShort(slotOffset + SLOT_LENGTH_FIELD, (short) 0);
        markAsDirty();
    }
}