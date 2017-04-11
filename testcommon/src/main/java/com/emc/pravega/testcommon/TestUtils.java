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
    private static final int MAX_PORT = 52767;
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(BASE_PORT);

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public synchronized static int getAvailableListenPort() {
        AtomicInteger candidatePort = new AtomicInteger(NEXT_PORT.intValue());

        for (;;) {
            candidatePort.incrementAndGet();

            if (candidatePort.intValue() > MAX_PORT) {
                candidatePort = new AtomicInteger(BASE_PORT);
            }

            if (candidatePort == NEXT_PORT) {
                log.error("Could not find free port in range {} - {}", BASE_PORT, MAX_PORT);
                return -1;
            }

            try {
                ServerSocket serverSocket = new ServerSocket(candidatePort.intValue());
                serverSocket.close();
                NEXT_PORT.set(candidatePort.intValue());
                return NEXT_PORT.intValue();
            } catch (IOException e) {
                log.error("Could not bind to port {} in range {} - {}", NEXT_PORT.intValue(), BASE_PORT, MAX_PORT);
                return -1;
            }
        }
    }
}
