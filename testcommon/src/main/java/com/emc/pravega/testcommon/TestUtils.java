/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.testcommon;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {
    private static final int BASE_PORT = 11221;
    private static final int MAX_PORT_COUNT = 41546;
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(1);

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public synchronized static int getAvailableListenPort() {
        int candidatePort = BASE_PORT + NEXT_PORT.getAndIncrement() % MAX_PORT_COUNT;
        for (int i = 0; i < MAX_PORT_COUNT; i++) {
            if (candidatePort > BASE_PORT + MAX_PORT_COUNT) {
                candidatePort = BASE_PORT;
            }
            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort);
                serverSocket.close();
                return candidatePort;
            } catch (IOException e) {
                candidatePort++;
            }
        }
        throw new IllegalStateException(
                String.format("Could not assign port in range %d - %d", BASE_PORT, MAX_PORT_COUNT + BASE_PORT));
    }
}
