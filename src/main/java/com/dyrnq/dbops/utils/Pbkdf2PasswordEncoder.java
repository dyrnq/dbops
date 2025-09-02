package com.dyrnq.dbops.utils;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

public class Pbkdf2PasswordEncoder {

    private final int iterations;
    private final int keyLength;
    private final SecureRandom random;

    public Pbkdf2PasswordEncoder(int iterations, int keyLength) {
        this.iterations = iterations;
        this.keyLength = keyLength;
        this.random = new SecureRandom();
    }

    public String encode(CharSequence rawPassword) {
        byte[] salt = generateSalt();
        byte[] encodedPassword = encode(rawPassword.toString().toCharArray(), salt);
        return bytesToHex(salt) + bytesToHex(encodedPassword);
    }

    public boolean matches(CharSequence rawPassword, String encodedPassword) {
        byte[] decodedPassword = hexToBytes(encodedPassword.substring(16));
        byte[] salt = hexToBytes(encodedPassword.substring(0, 16));
        byte[] hashedPassword = encode(rawPassword.toString().toCharArray(), salt);
        return slowEquals(hashedPassword, decodedPassword);
    }

    private byte[] generateSalt() {
        byte[] salt = new byte[8];
        random.nextBytes(salt);
        return salt;
    }

    private byte[] encode(char[] password, byte[] salt) {
        try {
            PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, keyLength * 8);
            SecretKeyFactory skf = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return skf.generateSecret(spec).getEncoded();
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException("Failed to encode password", e);
        }
    }

    private boolean slowEquals(byte[] a, byte[] b) {
        int diff = a.length ^ b.length;
        for (int i = 0; i < a.length && i < b.length; i++) {
            diff |= a[i] ^ b[i];
        }
        return diff == 0;
    }

    private byte[] hexToBytes(String hex) {
        int len = hex.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(hex.charAt(i), 16) << 4)
                                  + Character.digit(hex.charAt(i + 1), 16));
        }
        return data;
    }

    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

}
