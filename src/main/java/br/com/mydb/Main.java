package br.com.mydb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class Main {

    private static Database database;
    private static Map<String, Table> openTables = new HashMap<>();

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println("Erro: Forneça o nome do arquivo de banco de dados como argumento.");
            System.exit(1);
        }
        String databaseFilename = args[0];
        System.out.println("Usando o arquivo de banco de dados: " + databaseFilename);

        database = new Database(databaseFilename);

        Scanner scanner = new Scanner(System.in);

        while (true) {
            printPrompt();
            scanner.useDelimiter(";");
            String input = scanner.next().trim();

            if (input.isEmpty()) {
                continue;
            }

            if (input.startsWith(".")) {
                if (handleMetaCommand(input)) {
                    database.close(openTables);
                    break;
                }
                continue;
            }

            handleStatement(input);
        }

        scanner.close();
        System.out.println("Ate logo!");
    }

    /**
     * Imprime o prompt do banco de dados.
     */
    private static void printPrompt() {
        System.out.print("db > ");
    }

    /**
     * Processa meta-comandos (comandos que começam com '.').
     * @param input O comando digitado pelo usuário.
     * @return Retorna 'true' se o comando for para sair, 'false' caso contrário.
     */
    private static boolean handleMetaCommand(String input) {
        if (input.equals(".exit")) {
            return true;
        } else {
            System.out.println("Comando não reconhecido: " + input);
            return false;
        }
    }

    /**
     * Processa comandos SQL (ou similares).
     * @param input O comando digitado pelo usuário.
     */
    private static void handleStatement(String input) throws IOException {

            String[] parts = input.split(" ");
            String command = parts[0].toLowerCase();

            Table table;

            switch (command) {
                case "create":
                    if (parts.length != 4 || !parts[1].equals("table")) {
                        System.out.println("Comando inválido. Use: create table <nome> <tamanho_linha>");
                        return;
                    }
                    String tableName = parts[2];
                    try {
                        int rowSize = Integer.parseInt(parts[3]);
                        database.createTable(tableName, rowSize);
                        System.out.println("Tabela '" + tableName + "' criada com tamanho de linha " + rowSize + ".");
                    } catch (NumberFormatException e) {
                        System.out.println("Erro: O tamanho da linha deve ser um número.");
                    }
                    break;

                case "insert":
                    String targetTable = parts[1];
                    if (openTables.containsKey(targetTable)) {
                        table = openTables.get(targetTable);
                    } else {
                        table = database.openTable(targetTable);
                        openTables.put(targetTable, table);
                    }

                    int id = Integer.parseInt(parts[2]);
                    String username = parts[3];
                    String email = parts[4];
                    User user = new User(id, username, email);

                    table.insert(user.getId(), user.toBytes());
                    System.out.println("Inserido na tabela '" + targetTable + "'.");
                    break;

                case "select":
                    String tableToSelect = parts[1];
                    if (openTables.containsKey(tableToSelect)) {
                        table = openTables.get(tableToSelect);
                    } else {
                        table = database.openTable(tableToSelect);
                        openTables.put(tableToSelect, table);
                    }

                    byte[] foundBytes = table.find(Integer.parseInt(parts[2]));
                    if (foundBytes != null) {
                        User foundUser = User.fromBytes(foundBytes);
                        System.out.println("Encontrado: " + foundUser);
                    }

                    break;

                default:
                    System.out.println("Comando SQL nao reconhecido: " + command);
                    break;
            }
    }
}