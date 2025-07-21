package br.com.mydb;

public enum PageType {

    DATA_PAGE((byte) 0x0D),
    BTREE_LEAF_NODE((byte) 0x0L),
    BTREE_INTERNAL_NODE((byte) 0x01);

    public final byte value;

    PageType(byte value) {
        this.value = value;
    }

    /**
     * Converte um valor de byte lido do disco de volta para o tipo Enum correspondente.
     * @param value O byte lido do header da página.
     * @return O PageType correspondente.
     * @throws IllegalArgumentException se o byte não corresponder a nenhum tipo conhecido.
     */
    public static PageType fromValue(byte value) {
        for (PageType type : values()) {
            if (type.value == value) {
                return type;
            }
        }
        throw new IllegalArgumentException("Tipo de página desconhecido: " + value);
    }
}