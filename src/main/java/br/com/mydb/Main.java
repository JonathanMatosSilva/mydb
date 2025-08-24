package br.com.mydb;

import java.io.IOException;
import java.util.*;

public class Main {

    private static Database database;
    private static final Map<String, Table> openTables = new HashMap<>();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Erro: Forneça o nome do arquivo de banco de dados como argumento.");
            System.exit(1);
        }
        String databaseFilename = args[0];
        System.out.println("Usando o arquivo de banco de dados: " + databaseFilename);

        try {
            database = new Database(databaseFilename);
            Scanner scanner = new Scanner(System.in);
            System.out.println("Bem-vindo ao mydb. Digite .exit para sair.");

            while (true) {
                printPrompt();
                scanner.useDelimiter(";");
                String input = scanner.next().trim();

                if (input.isEmpty()) {
                    continue;
                }

                if (input.startsWith(".")) {
                    if (handleMetaCommand(input)) {
                        break;
                    }
                    continue;
                }

                handleStatement(input);
            }

            scanner.close();
            database.close(openTables);
            System.out.println("Até logo!");

        } catch (IOException e) {
            System.err.println("Erro de IO no banco de dados: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void printPrompt() {
        System.out.print("db > ");
    }

    private static boolean handleMetaCommand(String input) {
        if (input.equals(".exit")) {
            return true;
        } else {
            System.out.println("Comando não reconhecido: " + input);
            return false;
        }
    }

    private static Table getTable(String tableName) throws IOException {
        if (openTables.containsKey(tableName)) {
            return openTables.get(tableName);
        }
        Table table = database.openTable(tableName);
        openTables.put(tableName, table);
        return table;
    }

    private static void handleStatement(String input) throws IOException {
        String[] parts = input.split("\\s+", 2);
        String command = parts[0].toLowerCase();

        try {
            switch (command) {
                case "create":
                    handleCreateTable(parts[1]);
                    break;
                case "insert":
                    handleInsert(parts[1]);
                    break;
                case "select":
                    handleSelect(parts[1]);
                    break;
                case "delete":
                    handleDelete(parts[1]);
                    break;
                case "update":
                    handleUpdate(parts[1]);
                    break;
                default:
                    System.out.println("Comando SQL não reconhecido: " + command);
                    break;
            }
        } catch (Exception e) {
            System.out.println("Erro ao executar comando: " + e.getMessage());
            // e.printStackTrace(); // Descomente para depuração
        }
    }

    private static void handleCreateTable(String statement) throws IOException {
        statement = statement.replace("table", "").trim();
        int openParen = statement.indexOf('(');
        int closeParen = statement.lastIndexOf(')');

        if (openParen == -1 || closeParen == -1) {
            System.out.println("Sintaxe inválida. Use: create table <nome> (<col1> <tipo1>, ...);");
            return;
        }

        String tableName = statement.substring(0, openParen).trim();
        String colsPart = statement.substring(openParen + 1, closeParen).trim();
        String[] colDefs = colsPart.split(",");

        List<Column> schema = new ArrayList<>();
        int ordinalPosition = 1;
        for (String def : colDefs) {
            String[] parts = def.trim().split("\\s+");
            String colName = parts[0];
            String colType = parts[1].toUpperCase();
            DataType dataType = (colType.equals("INT")) ? DataType.INTEGER : DataType.VARCHAR;
            schema.add(new Column(colName, dataType, ordinalPosition++));
        }

        database.createTable(tableName, schema);
    }

    private static void handleInsert(String statement) throws IOException {
        // Ex: into usuarios values (1, 'João Silva')
        String[] parts = statement.split("values");
        String tableName = parts[0].replace("into", "").trim();
        String valuesPart = parts[1].trim();
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1); // Remove parênteses
        String[] values = valuesPart.split(",");

        Table table = getTable(tableName);
        List<Column> schema = table.getSchema();

        if (values.length != schema.size()) {
            System.out.println("Erro: O número de valores não corresponde ao número de colunas.");
            return;
        }

        Row newRow = new Row();
        int primaryKey = -1;

        for (int i = 0; i < schema.size(); i++) {
            Column col = schema.get(i);
            String valStr = values[i].trim().replace("'", ""); // Remove aspas simples

            Object value;
            if (col.type() == DataType.INTEGER) {
                value = Integer.parseInt(valStr);
            } else {
                value = valStr;
            }
            newRow.put(col.name(), value);

            if (col.ordinalPosition() == 1) { // Assume que a primeira coluna é a chave primária
                primaryKey = (Integer) value;
            }
        }

        if (primaryKey == -1) {
            System.out.println("Erro: Chave primária não encontrada ou não é a primeira coluna.");
            return;
        }

        table.insert(primaryKey, newRow);
        System.out.println("Inserido na tabela '" + tableName + "'.");
    }

    private static void handleSelect(String statement) throws IOException {
        String[] parts = statement.trim().split("\\s+");
        String tableName = parts[1];
        int key = Integer.parseInt(parts[2]);

        Table table = getTable(tableName);
        Row foundRow = table.find(key);

        if (foundRow != null) {
            System.out.println("Encontrado: " + foundRow);
        } else {
            System.out.println("Chave " + key + " não encontrada na tabela '" + tableName + "'.");
        }
    }

    private static void handleDelete(String statement) throws IOException {
        // Ex: from usuarios 1
        String[] parts = statement.trim().split("\\s+");
        String tableName = parts[1];
        int key = Integer.parseInt(parts[2]);

        Table table = getTable(tableName);
        table.delete(key);
    }

    private static void handleUpdate(String statement) throws IOException {
        String[] parts = statement.split("set");
        String tableName = parts[0].trim();
        String valuesPart = parts[1].trim();
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1); // Remove parênteses
        String[] values = valuesPart.split(",");

        Table table = getTable(tableName);
        List<Column> schema = table.getSchema();

        if (values.length != schema.size()) {
            System.out.println("Erro: O número de valores não corresponde ao número de colunas.");
            return;
        }

        Row updatedRow = new Row();
        int primaryKey = Integer.parseInt(values[0].trim());

        for (int i = 0; i < schema.size(); i++) {
            Column col = schema.get(i);
            String valStr = values[i].trim().replace("'", "");

            Object value;
            if (col.type() == DataType.INTEGER) {
                value = Integer.parseInt(valStr);
            } else {
                value = valStr;
            }
            updatedRow.put(col.name(), value);
        }

        table.update(primaryKey, updatedRow);
    }
}