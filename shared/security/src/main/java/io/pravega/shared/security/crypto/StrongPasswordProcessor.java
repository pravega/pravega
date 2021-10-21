/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.security.crypto;

import java.math.BigInteger;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import lombok.Builder;

/**
 * This class implements a a `PBKDF2WithHmacSHA1` based password digest creator and validator.
 *
 * Following steps are taken during the creation of the digest:
 *  1. A salt is generated.
 *  2. The password is encoded with this salt.
 *  3. Number of iterations, salt and this password is string encoded and concatenated with ":" as separator.
 *  4. This whole string is again string encoded with base 16.
 *
 *  For validation these steps are reversed to get the password digest from the stored password. The incoming password
 *  is digested with the retrieved iterations and salt. The generated digest is then cross checked against the created digest.
 */
@Builder
public class StrongPasswordProcessor {

    @Builder.Default
    private String keyAlgorythm = "PBKDF2WithHmacSHA256";
    @Builder.Default
    private int keyLength = 64 * 8;
    @Builder.Default
    private int saltLength = 32;
    @Builder.Default
    private int iterations = 5000;

    /*
     * @param password              The incoming password.
     * @param encryptedPassword     The stored password digest.
     * @return                      true if the password matches, false otherwise.
     * @throws NoSuchAlgorithmException encryption exceptions.
     * @throws InvalidKeySpecException encryption exceptions.
     */
    public boolean checkPassword(char[] password, String encryptedPassword)
            throws NoSuchAlgorithmException, InvalidKeySpecException {
        String storedPassword = new String(fromHex(encryptedPassword));
        String[] parts = storedPassword.split(":");
        int iterations = Integer.parseInt(parts[0]);
        byte[] salt = fromHex(parts[1]);
        byte[] hash = fromHex(parts[2]);

        PBEKeySpec spec = new PBEKeySpec(password, salt, iterations, keyLength);
        SecretKeyFactory skf = SecretKeyFactory.getInstance(keyAlgorythm);
        byte[] testHash = skf.generateSecret(spec).getEncoded();

        //This is time independent version of array comparison.
        // This is done to ensure that time based attacks do not happen.
        //Read more here for time based attacks in this context.
        // https://security.stackexchange.com/questions/74547/timing-attack-against-hmac-in-authenticated-encryption
        int diff = hash.length ^ testHash.length;
        for (int i = 0; i < hash.length && i < testHash.length; i++) {
            diff |= hash[i] ^ testHash[i];
        }
        return diff == 0;
    }


    /*
     * @param userPassword The password to be encrypted.
     * @return The encrypted string digest.
     * @throws NoSuchAlgorithmException encryption exceptions.
     * @throws InvalidKeySpecException encryption exceptions.
     */
    public String encryptPassword(String userPassword) throws NoSuchAlgorithmException, InvalidKeySpecException {
        char[] chars = userPassword.toCharArray();
        byte[] salt = getSalt();

        PBEKeySpec spec = new PBEKeySpec(chars, salt, iterations, keyLength);
        SecretKeyFactory skf = SecretKeyFactory.getInstance(keyAlgorythm);
        byte[] hash = skf.generateSecret(spec).getEncoded();
        return toHex((iterations + ":" + toHex(salt) + ":" + toHex(hash)).getBytes());
    }

    private byte[] getSalt() throws NoSuchAlgorithmException {
        SecureRandom sr = SecureRandom.getInstance("SHA1PRNG");
        byte[] salt = new byte[saltLength];
        sr.nextBytes(salt);
        return salt;
    }

    private String toHex(byte[] array) {
        BigInteger bi = new BigInteger(1, array);
        String hex = bi.toString(16);
        int paddingLength = (array.length * 2) - hex.length();
        if (paddingLength > 0) {
            return String.format("%0"  + paddingLength + "d", 0) + hex;
        } else {
            return hex;
        }
    }

    private byte[] fromHex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }
}
