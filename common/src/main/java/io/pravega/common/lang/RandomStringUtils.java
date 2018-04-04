/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.lang;

import io.pravega.common.hash.RandomFactory;
import java.util.Random;

public class RandomStringUtils {
    
    /**
     * <p>Creates a random string whose length is the number of characters
     * specified.</p>
     *
     * <p>Characters will be chosen from the set of Latin alphabetic
     * characters (a-z, A-Z) and the digits 0-9.</p>
     *
     * @param count  the length of random string to create
     * @return the random string
     */
    public static String randomAlphanumeric(final int count) {
        Random r = RandomFactory.create();
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            int randValue = r.nextInt(26 * 2 + 10);
            char newChar;
            if (randValue < 26) {
                newChar = (char) ((int) 'a' + randValue);
            } else if (randValue < 26 * 2) {
                newChar = (char) ((int) 'A' + (randValue - 26));
            } else {
                newChar = (char) ((int) '0' + (randValue - 26 * 2));
            }
            sb.append(newChar);
        }
        return sb.toString();
    }
    
    /**
     * <p>Creates a random string whose length is the number of characters
     * specified.</p>
     *
     * <p>Characters will be chosen from the set of Latin alphabetic
     * characters (a-z, A-Z).</p>
     *
     * @param count  the length of random string to create
     * @return the random string
     */
    public static String randomAlphabetic(final int count) {
        Random r = RandomFactory.create();
        StringBuilder sb = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            int randValue = r.nextInt(26 * 2);
            char newChar;
            if (randValue < 26) {
                newChar = (char) ((int) 'a' + randValue);
            } else {
                newChar = (char) ((int) 'A' + (randValue - 26));
            }
            sb.append(newChar);
        }
        return sb.toString();
    }
    
}
