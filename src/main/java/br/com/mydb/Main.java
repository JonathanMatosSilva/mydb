package br.com.mydb;

import java.awt.BorderLayout;
import java.awt.AWTError;
import java.awt.Color;
import java.awt.Font;
import java.awt.GraphicsEnvironment;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import javax.swing.AbstractAction;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JTextField;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

public class Main {

    private static final String DB_PADRAO = "mydb.bd";
    private static Database database;
    private static final Map<String, Table> openTables = new HashMap<>();

    public static void main(String[] args) {
        String arquivoDb = pegarArquivoDb(args);
        System.out.println("Usando o arquivo de banco de dados: " + arquivoDb);

        try {
            database = new Database(arquivoDb);
            Scanner scanner = new Scanner(System.in);

            if (login(scanner)) {
                abrirConsoleDb(scanner);
            }

            scanner.close();
            database.close(openTables);
            System.out.println("Até logo!");

        } catch (IOException e) {
            System.err.println("Erro de IO no banco de dados: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String pegarArquivoDb(String[] args) {
        if (args.length > 0) {
            return args[0];
        }

        return DB_PADRAO;
    }

    private static boolean login(Scanner scanner) throws IOException {
        if (GraphicsEnvironment.isHeadless()) {
            return loginNoTerminal(scanner);
        }

        try {
            usarVisualDoSistema();
            final boolean[] loginOk = { false };
            SwingUtilities.invokeAndWait(() -> loginOk[0] = abrirTelaLogin());
            return loginOk[0];
        } catch (AWTError e) {
            return loginNoTerminal(scanner);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();

            if (cause instanceof AWTError) {
                return loginNoTerminal(scanner);
            }
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }

            throw new IOException("Erro ao exibir a interface de login.", cause);
        }
    }

    private static boolean loginNoTerminal(Scanner scanner) {
        System.out.print("Usuário: ");
        String usuario = scanner.nextLine().trim();

        System.out.print("Senha: ");
        String senha = scanner.nextLine().trim();

        ResultadoLogin resultado = validarLogin(usuario, senha);

        if (!resultado.ok()) {
            System.out.println(resultado.msg());
        }

        return resultado.ok();
    }

    private static ResultadoLogin validarLogin(String usuario, String senha) {
        try {
            Table tabelaUsers = database.openTable("db_users");
            Row userRow = tabelaUsers.find(usuario.hashCode());

            if (userRow == null) {
                return new ResultadoLogin(false, "Usuário não encontrado.");
            }
            String hashSalvo = (String) userRow.get("password");
            String hashDigitado = Util.hashPassword(senha, usuario);

            if (hashSalvo.equals(hashDigitado)) {
                return new ResultadoLogin(true, "");
            }

            return new ResultadoLogin(false, "Senha incorreta.");
        } catch (IOException e) {
            return new ResultadoLogin(false, "Erro crítico: Tabela de usuários não encontrada. O banco pode estar corrompido.");
        }
    }

    private static void usarVisualDoSistema() {
        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
        }
    }

    private static boolean abrirTelaLogin() {
        final boolean[] loginOk = { false };

        JDialog janela = new JDialog((java.awt.Frame) null, "Login - mydb", true);
        janela.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);

        JPanel tela = new JPanel(new BorderLayout(0, 18));
        tela.setBorder(BorderFactory.createEmptyBorder(24, 28, 24, 28));

        JLabel titulo = new JLabel("mydb");
        titulo.setFont(titulo.getFont().deriveFont(Font.BOLD, 24f));
        tela.add(titulo, BorderLayout.NORTH);

        JPanel form = new JPanel(new GridBagLayout());
        GridBagConstraints layout = new GridBagConstraints();
        layout.insets = new Insets(6, 0, 6, 0);
        layout.fill = GridBagConstraints.HORIZONTAL;
        layout.weightx = 1;

        JTextField campoUser = new JTextField(22);
        JPasswordField campoSenha = new JPasswordField(22);
        JLabel msgErro = new JLabel(" ");
        msgErro.setForeground(new Color(176, 0, 32));

        addCampo(form, layout, 0, "Usuário", campoUser);
        addCampo(form, layout, 1, "Senha", campoSenha);

        layout.gridx = 0;
        layout.gridy = 4;
        layout.gridwidth = 2;
        form.add(msgErro, layout);

        tela.add(form, BorderLayout.CENTER);

        JButton btnEntrar = new JButton("Entrar");
        JButton btnCancelar = new JButton("Cancelar");
        JPanel botoes = new JPanel(new GridBagLayout());

        GridBagConstraints layoutBotoes = new GridBagConstraints();
        layoutBotoes.insets = new Insets(0, 6, 0, 0);
        layoutBotoes.gridx = 0;
        layoutBotoes.gridy = 0;
        botoes.add(btnCancelar, layoutBotoes);

        layoutBotoes.gridx = 1;
        botoes.add(btnEntrar, layoutBotoes);

        tela.add(botoes, BorderLayout.SOUTH);

        Runnable tentarEntrar = () -> {
            String usuario = campoUser.getText().trim();
            String senha = new String(campoSenha.getPassword());

            if (usuario.isEmpty() || senha.isEmpty()) {
                msgErro.setText("Informe usuário e senha.");
                return;
            }

            ResultadoLogin resultado = validarLogin(usuario, senha);

            if (resultado.ok()) {
                loginOk[0] = true;
                janela.dispose();
                return;
            }

            msgErro.setText(resultado.msg());
            campoSenha.setText("");
            campoSenha.requestFocusInWindow();
        };

        btnEntrar.addActionListener(event -> tentarEntrar.run());
        btnCancelar.addActionListener(event -> janela.dispose());
        campoSenha.addActionListener(event -> tentarEntrar.run());
        janela.getRootPane().setDefaultButton(btnEntrar);
        escFecha(janela);

        janela.setContentPane(tela);
        janela.pack();
        janela.setResizable(false);
        janela.setLocationRelativeTo(null);
        SwingUtilities.invokeLater(campoUser::requestFocusInWindow);
        janela.setVisible(true);

        return loginOk[0];
    }

    private static void addCampo(JPanel painel, GridBagConstraints layout, int linha, String label,
            JComponent campo) {
        layout.gridx = 0;
        layout.gridy = linha * 2;
        layout.gridwidth = 2;
        painel.add(new JLabel(label), layout);

        layout.gridy = linha * 2 + 1;
        painel.add(campo, layout);
    }

    private static void escFecha(JDialog janela) {
        janela.getRootPane()
                .getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW)
                .put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "close");
        janela.getRootPane()
                .getActionMap()
                .put("close", new AbstractAction() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        janela.dispose();
                    }
                });
    }

    private static void abrirConsoleDb(Scanner scanner) throws IOException {
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
    }

    private record ResultadoLogin(boolean ok, String msg) {
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
        String[] parts = statement.split("values");
        String tableName = parts[0].replace("into", "").trim();
        String valuesPart = parts[1].trim();
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1);
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
            String valStr = values[i].trim().replace("'", "");

            Object value;
            if (col.type() == DataType.INTEGER) {
                value = Integer.parseInt(valStr);
            } else {
                value = valStr;
            }
            newRow.put(col.name(), value);

            if (col.ordinalPosition() == 1) {
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

        if (parts[0].equals("*") && parts[1].equals("from")) {
            String tableName = parts[2];
            Table table = getTable(tableName);

            List<Row> results = new ArrayList<>();
            Cursor cursor = table.start();
            while (!cursor.isEndOfTable()) {
                results.add(cursor.getRecord());
                cursor.advance();
            }

            if (results.isEmpty()) {
                System.out.println("Tabela '" + tableName + "' está vazia ou não contém registros.");
            } else {
                TableFormatter.printTable(table.getSchema(), results);
            }
        } else if (parts[0].equals("from")) {
            String tableName = parts[1];
            int key = Integer.parseInt(parts[2]);

            Table table = getTable(tableName);
            Row foundRow = table.find(key);

            if (foundRow != null) {
                TableFormatter.printTable(table.getSchema(), Collections.singletonList(foundRow));
            } else {
                System.out.println("Chave " + key + " não encontrada na tabela '" + tableName + "'.");
            }

        } else {
            System.out.println("Sintaxe de SELECT inválida. Use: select * from <tabela>; ou select from <tabela> <chave>;");
        }
    }

    private static void handleDelete(String statement) throws IOException {
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
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1);
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
