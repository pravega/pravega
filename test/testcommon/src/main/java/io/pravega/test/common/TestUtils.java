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

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {
    // Linux uses ports from range 32768 - 61000.
    private static final int BASE_PORT = 32768;
    private static final int MAX_PORT_COUNT = 28232;
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(1);

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
}
