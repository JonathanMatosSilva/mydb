package br.com.mydb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

public class Pager {

    private final RandomAccessFile databaseFile;
    private final int pageSize;
    private int numPages;

    private final Map<Integer, Page> pageCache;


    public Pager(String databaseFilePath, int pageSize) throws IOException {
        File file = new File(databaseFilePath);
        this.databaseFile = new RandomAccessFile(file, "rw");
        this.pageSize = pageSize;

        long fileSize = this.databaseFile.length();
        this.numPages = (int) (fileSize / pageSize);

        this.pageCache = new HashMap<>();
    }

    public Page getPage(int pageNumber) throws IOException {
        if (pageNumber < 0 || pageNumber >= numPages) {
            throw new IllegalArgumentException("Número de página inválido: " + pageNumber);
        }

        if (pageCache.containsKey(pageNumber)) {
            return pageCache.get(pageNumber);
        }

        long offset = (long) pageNumber * pageSize;
        byte[] data = new byte[pageSize];
        databaseFile.seek(offset);
        databaseFile.readFully(data);

        Page page = new Page(pageNumber, data);
        pageCache.put(pageNumber, page);
        return page;
    }


    public Page newPage() throws IOException {
        byte[] data = new byte[pageSize];
        Page page = new Page(numPages, data);
        pageCache.put(numPages, page);
        numPages++;
        return page;
    }

    public void flushPage(Page page) throws IOException {
        long offset = (long) page.getPageNumber() * pageSize;
        databaseFile.seek(offset);
        byte[] data = page.getBytes();
        databaseFile.write(data);
    }

    public void close() throws IOException {
        for (Map.Entry<Integer, Page> entry : pageCache.entrySet()) {
            if (entry.getValue().isDirty()) {
                flushPage(entry.getValue());
            }
        }
    }

    public int getNumPages() {
        return numPages;
    }

    public int getPageSize() {
        return pageSize;
    }

    public byte[] readBytesAt(long offset, int length) throws IOException {
        databaseFile.seek(offset);
        byte[] data = new byte[length];
        databaseFile.readFully(data);
        return data;
    }
}
