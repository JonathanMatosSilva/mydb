package br.com.mydb;

public enum DataType {
    INTEGER(1, 4),
    VARCHAR(2, -1);

    public final int id;
    public final int sizeInBytes;

    DataType(int id, int sizeInBytes) {
        this.id = id;
        this.sizeInBytes = sizeInBytes;
    }

    public static DataType fromId(int id) {
        for (DataType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("ID de DataType inválido: " + id);
    }
}