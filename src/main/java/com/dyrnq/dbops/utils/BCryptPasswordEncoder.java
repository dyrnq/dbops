package com.dyrnq.dbops.utils;

import cn.hutool.crypto.digest.BCrypt;

import java.security.SecureRandom;

public class BCryptPasswordEncoder {
    public static final int DEFAULT_STRENGTH = 12;
    private final SecureRandom random;
    private final int strength;

    public BCryptPasswordEncoder(int strength) {
        this.random = new SecureRandom();
        this.strength = strength;
    }

    public String encode(CharSequence rawPassword) {
        String salt = BCrypt.gensalt(strength, random);
        return BCrypt.hashpw(rawPassword.toString(), salt);
    }

    public boolean matches(CharSequence rawPassword, String encodedPassword) {
        return BCrypt.checkpw(rawPassword.toString(), encodedPassword);
    }
}
