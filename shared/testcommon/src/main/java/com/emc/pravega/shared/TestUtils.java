/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.shared;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Utility class for Tests.
 */
@Slf4j
public class TestUtils {

    /**
     * A helper method to get a random free port.
     *
     * @return free port.
     */
    public static int getAvailableListenPort() {
        ServerSocket serverSocket = null;
        int freePort;
        try {
            serverSocket = new ServerSocket(0);
            freePort = serverSocket.getLocalPort();
            serverSocket.close();
            return freePort;
        } catch (IOException e) {
            log.error("Could not find free port");
            return -1;
        }
    }
}
