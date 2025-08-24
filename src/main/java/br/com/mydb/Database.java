package br.com.mydb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class Database {

    private final Pager pager;
    private Table tablesCatalog;
    private Table columnsCatalog;

    private static final int TABLES_CATALOG_DATA_PAGE = 2;
    private static final int COLUMNS_CATALOG_DATA_PAGE = 4;

    private static final int TABLES_CATALOG_ROOT_OFFSET = 0;
    private static final int COLUMNS_CATALOG_ROOT_OFFSET = 4;

    private static final List<Column> TABLES_CATALOG_SCHEMA;
    static {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("tableName", DataType.VARCHAR, 1));
        schema.add(new Column("rootPageNumber", DataType.INTEGER, 2));
        schema.add(new Column("firstDataPageNumber", DataType.INTEGER, 3));
        TABLES_CATALOG_SCHEMA = Collections.unmodifiableList(schema);
    }

    private static final List<Column> COLUMNS_CATALOG_SCHEMA;
    static {
        List<Column> schema = new ArrayList<>();
        schema.add(new Column("tableHash", DataType.INTEGER, 1));
        schema.add(new Column("columnName", DataType.VARCHAR, 2));
        schema.add(new Column("dataTypeId", DataType.INTEGER, 3));
        schema.add(new Column("ordinalPosition", DataType.INTEGER, 4));
        COLUMNS_CATALOG_SCHEMA = Collections.unmodifiableList(schema);
    }

    public Database(String databaseFilePath) throws IOException {
        this.pager = new Pager(databaseFilePath, 4096);

        if (pager.getNumPages() == 0) {
            initializeNewDatabase();
        } else {
            Page headerPage = pager.getPage(0);
            ByteBuffer headerBuffer = ByteBuffer.wrap(headerPage.getBytes());
            int tablesRoot = headerBuffer.getInt(TABLES_CATALOG_ROOT_OFFSET);
            int columnsRoot = headerBuffer.getInt(COLUMNS_CATALOG_ROOT_OFFSET);

            this.tablesCatalog = new Table(pager, tablesRoot, TABLES_CATALOG_DATA_PAGE, TABLES_CATALOG_SCHEMA);
            this.columnsCatalog = new Table(pager, columnsRoot, COLUMNS_CATALOG_DATA_PAGE, COLUMNS_CATALOG_SCHEMA);
        }
    }

    public Table openTable(String tableName) throws IOException {
        int key = tableName.hashCode();
        Row row = tablesCatalog.find(key);

        if (row == null) {
            throw new IOException("Tabela '" + tableName + "' não encontrada.");
        }
        List<Column> schema = loadSchema(tableName);

        return new Table(this.pager, (Integer) row.get("rootPageNumber"), (Integer) row.get("firstDataPageNumber"), schema);
    }

    private List<Column> loadSchema(String tableName) throws IOException {
        List<Column> schema = new ArrayList<>();
        int tableKey = tableName.hashCode();

        Cursor cursor = columnsCatalog.start();

        while (!cursor.isEndOfTable()) {
            Row row = cursor.getRecord();
            int recordTableHash = (Integer) row.get("tableHash");

            if (recordTableHash == tableKey) {
                String columnName = (String) row.get("columnName");
                int dataTypeId = (Integer) row.get("dataTypeId");
                int ordinalPosition = (Integer) row.get("ordinalPosition");

                schema.add(new Column(
                        columnName,
                        DataType.fromId(dataTypeId),
                        ordinalPosition
                ));
            }
            cursor.advance();
        }
        schema.sort(Comparator.comparingInt(Column::ordinalPosition));
        return schema;
    }

    private void initializeNewDatabase() throws IOException {
        if (pager.getNumPages() != 0) {
            throw new IllegalStateException("A inicialização só pode ocorrer em um banco de dados vazio.");
        }
        // Página 0: Cabeçalho
        Page headerPage = pager.newPage();

        // Página 1 e 2: Catálogo de Tabelas
        Page tablesRootPage = pager.newPage().initializeAsLeaf();
        Page tablesDataPage = pager.newPage().initializeAsDataPage();

        // Página 3 e 4: Catálogo de Colunas
        Page columnsRootPage = pager.newPage().initializeAsLeaf();
        Page columnsDataPage = pager.newPage().initializeAsDataPage();

        ByteBuffer headerBuffer = ByteBuffer.wrap(headerPage.getBytes());
        headerBuffer.putInt(TABLES_CATALOG_ROOT_OFFSET, tablesRootPage.getPageNumber());
        headerBuffer.putInt(COLUMNS_CATALOG_ROOT_OFFSET, columnsRootPage.getPageNumber());
        pager.flushPage(headerPage);

        this.tablesCatalog = new Table(pager, tablesRootPage.getPageNumber(), tablesDataPage.getPageNumber(), TABLES_CATALOG_SCHEMA);
        this.columnsCatalog = new Table(pager, columnsRootPage.getPageNumber(), columnsDataPage.getPageNumber(), COLUMNS_CATALOG_SCHEMA);
    }

    public void createTable(String tableName, List<Column> schema) throws IOException {
        int tableKey = tableName.hashCode();
        if (tablesCatalog.find(tableKey) != null) {
            System.out.println("Essa tabela já existe.");
            return;
        }

        Page newTableRootPage = pager.newPage().initializeAsLeaf();
        Page firstDataPage = pager.newPage().initializeAsDataPage();
        pager.flushPage(newTableRootPage);
        pager.flushPage(firstDataPage);

        Row tableInfoRow = new Row();
        tableInfoRow.put("tableName", tableName);
        tableInfoRow.put("rootPageNumber", newTableRootPage.getPageNumber());
        tableInfoRow.put("firstDataPageNumber", firstDataPage.getPageNumber());

        tablesCatalog.insert(tableKey, tableInfoRow);

        for (Column col : schema) {
            int colKey = (tableName + "." + col.name()).hashCode();

            Row columnInfoRow = new Row();
            columnInfoRow.put("tableHash", tableKey);
            columnInfoRow.put("columnName", col.name());
            columnInfoRow.put("dataTypeId", col.type().id);
            columnInfoRow.put("ordinalPosition", col.ordinalPosition());

            columnsCatalog.insert(colKey, columnInfoRow);
        }
        System.out.println("Tabela '" + tableName + "' criada.");
    }

    public void close(Map<String, Table> openTables) throws IOException {
        System.out.println("Iniciando o fechamento do banco de dados...");

        for (Map.Entry<String, Table> entry : openTables.entrySet()) {
            String tableName = entry.getKey();
            Table table = entry.getValue();
            int tableKey = tableName.hashCode();

            System.out.println("-> Salvando estado da tabela '" + tableName + "'...");

            Row updatedTableInfoRow = new Row();
            updatedTableInfoRow.put("tableName", tableName);
            updatedTableInfoRow.put("rootPageNumber", table.getRootPageNumber());
            updatedTableInfoRow.put("firstDataPageNumber", table.getFirstDataPageNumber());

            this.tablesCatalog.update(tableKey, updatedTableInfoRow);
        }

        System.out.println("-> Salvando estado dos catálogos...");
        Page headerPage = pager.getPage(0);
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerPage.getBytes());

        headerBuffer.putInt(TABLES_CATALOG_ROOT_OFFSET, this.tablesCatalog.getRootPageNumber());
        headerBuffer.putInt(COLUMNS_CATALOG_ROOT_OFFSET, this.columnsCatalog.getRootPageNumber());
        pager.flushPage(headerPage);

        this.pager.close();
        System.out.println("Banco de dados fechado com sucesso.");
    }
}
