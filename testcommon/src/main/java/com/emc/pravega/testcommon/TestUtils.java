/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.testcommon;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Random;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {

    static final Random RAND = new Random();

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public static int randomPort() {
        int minValue = 1024;
        int maxValue = 49151 - minValue;
        ServerSocket serverSocket = null;

        for (int i = 0; i < 10000; i++) {
            try {
                serverSocket = new ServerSocket(RAND.nextInt(maxValue) + minValue);
                return serverSocket.getLocalPort();
            } catch (IOException ex) {
                continue; // try next port
            } finally {
                if (serverSocket != null) {
                    try {
                        serverSocket.close();
                    } catch (IOException e) {
                        log.error("Could not close ServerSocket");
                    }
                }
            }
        }

        log.error("Could not find free port between {} and {}", minValue, maxValue+minValue);
        return -1;
    }
}
