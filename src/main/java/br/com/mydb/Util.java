package br.com.mydb;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class Util {

    public static String hashPassword(String password, String salt) {
        try {

            MessageDigest msgDigest = MessageDigest.getInstance("SHA-256");
            String saltedPassword = salt + password;

            byte[] hash = msgDigest.digest(saltedPassword.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Não foi possível encontrar o algoritmo de hash", e);
        }
    }
}
