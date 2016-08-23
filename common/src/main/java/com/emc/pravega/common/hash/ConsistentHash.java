package com.emc.pravega.common.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ConsistentHash {

    static final String MD_5 = "MD5";

    public static int hash(String str, int numOfPoints) {
        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance(MD_5);
            byte[] data = str.getBytes();
            md.update(data, 0, data.length);
            BigInteger i = new BigInteger(1,md.digest());
            return i.intValue() % numOfPoints;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
