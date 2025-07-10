package br.com.mydb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
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

        String[] parts = input.split(" ");
        String command = parts[0].toLowerCase();

        switch (command) {
            case "insert":
                if (parts.length > 1) {

                    int id = Integer.parseInt(parts[1]);
                    String username = parts[2];
                    String email = parts[3];
                    User user = new User(id, username, email);

                    executeInsert(user);
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

    private static void executeInsert(User user) {

        ByteBuffer buffer = ByteBuffer.allocateDirect(User.ROW_SIZE);
        buffer.order(ByteOrder.nativeOrder());

        buffer.putInt(User.ID_OFFSET, user.getId());
        buffer.put(User.USERNAME_OFFSET, user.getUsername().getBytes());
        buffer.put(User.EMAIL_OFFSET, user.getEmail().getBytes());

        System.out.println("Bloco de memória preenchido.");
        System.out.println("------------------------------------");

        int idLido = buffer.getInt(User.ID_OFFSET);

        byte[] usernameBytes = new byte[User.USERNAME_SIZE];
        buffer.position(User.USERNAME_OFFSET);
        buffer.get(usernameBytes);
        String usernameLido = new String(usernameBytes, StandardCharsets.UTF_8).trim();

        byte[] emailBytes = new byte[User.EMAIL_OFFSET];
        buffer.position(User.EMAIL_OFFSET);
        buffer.get(emailBytes);
        String emailLido = new String(emailBytes, StandardCharsets.UTF_8).trim();

        System.out.println("Valores lidos: ID=" + idLido + ", username=" + usernameLido + ", email=" + emailLido);

        assert idLido == user.getId();
        assert usernameLido.equals(user.getUsername());
        assert emailLido.equals(user.getEmail());
    }
}