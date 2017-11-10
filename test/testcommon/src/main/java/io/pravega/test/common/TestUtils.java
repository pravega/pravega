/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.common;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {
    // Linux uses ports from range 32768 - 61000.
    private static final int BASE_PORT = 32768;
    private static final int MAX_PORT_COUNT = 28233;

    // We use a random start position here to avoid ports conflicts when this method is executed from multiple processes
    // in parallel. This is needed since the processes will contend for the same port sequence.
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(new Random().nextInt(MAX_PORT_COUNT));

    /**
     * We use this as a source of alphanumeric characters when generating random strings. Characters will be selected at
     * random from this string.
     */
    private static final char[] ALPHANUMERIC_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
    @GuardedBy("RANDOM")
    private static final Random RANDOM = new Random(0);

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public static int getAvailableListenPort() {
        for (int i = 0; i < MAX_PORT_COUNT; i++) {
            int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort);
                serverSocket.close();
                return candidatePort;
            } catch (IOException e) {
                // Do nothing. Try another port.
            }
        }
        throw new IllegalStateException(
                String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
    }

    /**
     * Generates a new alphanumeric String of the given length.
     *
     * @param length The length of the String.
     * @return The new String.
     */
    public static String randomAlphanumeric(int length) {
        return randomString(length, 0, ALPHANUMERIC_CHARS.length);
    }

    /**
     * Generates a new String containing only letters of the given length.
     *
     * @param length The length of the String.
     * @return The new String.
     */
    public static String randomAlphabetic(int length) {
        return randomString(length, 10, ALPHANUMERIC_CHARS.length);
    }

    /**
     * Generates a new String of the given length containing only characters of the given range.
     *
     * @param length              The length of the String.
     * @param lowerIndexInclusive The first Index (inclusive) in ALPHANUMERIC_CHARS to select characters from.
     * @param upperIndexExclusive The last index (exclusive) in ALPHANUMERIC_CHARS to select characters from.
     * @return The new String.
     */
    private static String randomString(int length, int lowerIndexInclusive, int upperIndexExclusive) {
        char[] data = new char[length];
        for (int i = 0; i < length; i++) {
            synchronized (RANDOM) {
                data[i] = ALPHANUMERIC_CHARS[lowerIndexInclusive + RANDOM.nextInt(upperIndexExclusive - lowerIndexInclusive)];
            }
        }

        return new String(data);
    }
}
