package br.com.mydb;

public class PromotedKey {

    private int key;
    private int rightChildPageNumber;

    public PromotedKey(int key, int rightChildPageNumber) {
        this.key = key;
        this.rightChildPageNumber = rightChildPageNumber;
    }

    public int getKey() {
        return key;
    }

    public int getRightChildPageNumber() {
        return rightChildPageNumber;
    }
}
