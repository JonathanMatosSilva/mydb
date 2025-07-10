package br.com.mydb;

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

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", username='" + username + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
