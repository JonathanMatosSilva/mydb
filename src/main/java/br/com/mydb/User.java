package br.com.mydb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class User {

    public static final int ID_SIZE = 4;
    public static final int USERNAME_SIZE = 32;
    public static final int EMAIL_SIZE = 255;

    public static final int ID_OFFSET = 0;
    public static final int USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
    public static final int EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;

    public static final int ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

    private int id;
    private String username;
    private String email;

    public User() {}

    public User(int id, String username, String email) {
        this.id = id;
        this.username = username;
        this.email = email;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(User.ROW_SIZE);
        buffer.order(ByteOrder.nativeOrder());

        buffer.putInt(User.ID_OFFSET, id);
        buffer.put(User.USERNAME_OFFSET, username.getBytes());
        buffer.put(User.EMAIL_OFFSET, email.getBytes());

        return buffer.array();
    }

    public static User fromBytes(byte[] rowData) {
        ByteBuffer buffer = ByteBuffer.wrap(rowData);
        buffer.order(ByteOrder.nativeOrder());

        int idLido = buffer.getInt(User.ID_OFFSET);

        byte[] usernameBytes = new byte[User.USERNAME_SIZE];
        buffer.position(User.USERNAME_OFFSET);
        buffer.get(usernameBytes);
        String usernameLido = new String(usernameBytes, StandardCharsets.UTF_8).trim();

        byte[] emailBytes = new byte[User.EMAIL_SIZE];
        buffer.position(User.EMAIL_OFFSET);
        buffer.get(emailBytes);
        String emailLido = new String(emailBytes, StandardCharsets.UTF_8).trim();

        return new User(idLido, usernameLido, emailLido);
    }

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
