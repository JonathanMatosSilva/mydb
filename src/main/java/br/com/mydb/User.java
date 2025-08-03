package br.com.mydb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class User {

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
        byte[] usernameBytes = username.getBytes(StandardCharsets.UTF_8);
        byte[] emailBytes = email.getBytes(StandardCharsets.UTF_8);

        int totalSize = 4 + usernameBytes.length + 4 + emailBytes.length + 4;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);

        buffer.putInt(usernameBytes.length);
        buffer.put(usernameBytes);

        buffer.putInt(emailBytes.length);
        buffer.put(emailBytes);

        buffer.putInt(id);

        return buffer.array();
    }

    public static User fromBytes(byte[] rowData) {
        ByteBuffer buffer = ByteBuffer.wrap(rowData);

        int usernameLength = buffer.getInt();
        byte[] usernameBytes = new byte[usernameLength];
        buffer.get(usernameBytes);
        String username = new String(usernameBytes, StandardCharsets.UTF_8);

        int emailLength = buffer.getInt();
        byte[] emailBytes = new byte[emailLength];
        buffer.get(emailBytes);
        String email = new String(emailBytes, StandardCharsets.UTF_8);

        int id = buffer.getInt();

        return new User(id, username, email);
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
