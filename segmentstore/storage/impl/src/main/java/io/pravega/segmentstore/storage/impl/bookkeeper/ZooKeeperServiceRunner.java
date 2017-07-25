/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.lang.ProcessHelpers;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Helps run ZooKeeper Server in process.
 */
@RequiredArgsConstructor
@Slf4j
public class ZooKeeperServiceRunner implements AutoCloseable {
    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private final AtomicReference<ZooKeeperServer> server = new AtomicReference<>();
    private final int zkPort;
    private File tmpDir;

    @Override
    public void close() throws Exception {
        if (this.server.get() != null) {
            this.server.get().shutdown();
        }

        if (this.tmpDir != null) {
            log.info("Cleaning up " + this.tmpDir);
            FileUtils.deleteDirectory(this.tmpDir);
        }
    }

    /**
     * Starts the ZooKeeper Service in process.
     *
     * @throws Exception If an exception occurred.
     */
    public void start() throws Exception {
        this.tmpDir = IOUtils.createTempDir("zookeeper", "inproc");
        this.tmpDir.deleteOnExit();

        val s = new ZooKeeperServer(this.tmpDir, this.tmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        this.server.set(s);
        val serverFactory = new NIOServerCnxnFactory();

        val address = LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort;
        log.info("Starting Zookeeper server at " + address + " ...");
        serverFactory.configure(new InetSocketAddress(LOOPBACK_ADDRESS, this.zkPort), 1000);
        serverFactory.startup(s);

        boolean b = waitForServerUp(this.zkPort);
        log.info("ZooKeeper server {}.", b ? "up" : "not up");
    }

    /**
     * Blocks the current thread and awaits ZooKeeper to start running locally on the given port.
     *
     * @param zkPort The ZooKeeper Port.
     * @return True if ZooKeeper started within a specified timeout, false otherwise.
     */
    public static boolean waitForServerUp(int zkPort) {
        val address = LOOPBACK_ADDRESS.getHostAddress() + ":" + zkPort;
        return LocalBookKeeper.waitForServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT);
    }

    //region Out-of-Process

    /**
     * Starts the ZooKeeper Service out of process.
     *
     * @param zkPort The port to start ZooKeeper on.
     * @return A Process reference.
     * @throws IOException If an exception occurred.
     */
    public static Process startOutOfProcess(int zkPort) throws IOException {
        return ProcessHelpers.execSimple(ZooKeeperServiceRunner.class, zkPort);
    }

    public static void main(String[] args) throws Exception {
        int port;
        try {
            port = Integer.parseInt(args[0]);
        } catch (Exception ex) {
            System.out.println(String.format("Invalid or missing arguments. Expected: ZKPort (int) (%s).", ex.getMessage()));
            System.exit(-1);
            return;
        }

        ZooKeeperServiceRunner runner = new ZooKeeperServiceRunner(port);
        runner.start();
        Thread.sleep(Long.MAX_VALUE);
    }

    //endregion
}
