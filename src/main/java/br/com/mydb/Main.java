package br.com.mydb;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Erro: Forneça o nome do arquivo de banco de dados como argumento.");
            System.exit(1);
        }
        String databaseFilename = args[0];
        System.out.println("Usando o arquivo de banco de dados: " + databaseFilename);

        Scanner scanner = new Scanner(System.in);

        while (true) {
            printPrompt();
            String input = scanner.nextLine().trim();

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
    private static void handleStatement(String input) {

        String[] parts = input.split(" ", 2);
        String command = parts[0].toLowerCase();

        switch (command) {
            case "insert":
                System.out.println("Executando um INSERT... (logica a ser implementada)");
                if (parts.length > 1) {
                    System.out.println("  -> Dados: " + parts[1]);
                }
                break;

            case "select":
                System.out.println("Executando um SELECT... (logica a ser implementada)");
                break;

            default:
                System.out.println("Comando SQL nao reconhecido: " + command);
                break;
        }
    }
}