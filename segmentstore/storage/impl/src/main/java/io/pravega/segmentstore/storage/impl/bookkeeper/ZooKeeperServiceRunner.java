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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Helps run ZooKeeper Server in process.
 */
@RequiredArgsConstructor
@Slf4j
public class ZooKeeperServiceRunner implements AutoCloseable {
    public static final String PROPERTY_ZK_PORT = "zkPort";
    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private final AtomicReference<ZooKeeperServer> server = new AtomicReference<>();
    private final AtomicReference<NIOServerCnxnFactory> serverFactory = new AtomicReference<>();
    private final int zkPort;
    private final AtomicReference<File> tmpDir = new AtomicReference<>();

    @Override
    public void close() {
        stop();

        File t = this.tmpDir.getAndSet(null);
        if (t != null) {
            log.info("Cleaning up " + t);
            try {
                FileUtils.deleteDirectory(t);
            } catch (IOException ex) {
                // There is an exception thrown when run under Windows only. As this is not critical to our execution,
                // ignore it and log a warning.
                log.warn("Unable to delete temp directory '{}'.", t, ex);
            }
        }
    }

    public void initialize() throws IOException {
        System.setProperty("zookeeper.4lw.commands.whitelist", "*"); // As of ZooKeeper 3.5 this is needed to not break start()
        if (this.tmpDir.compareAndSet(null, IOUtils.createTempDir("zookeeper", "inproc"))) {
            this.tmpDir.get().deleteOnExit();
        }
    }

    /**
     * Starts the ZooKeeper Service in process.
     *
     * @throws Exception If an exception occurred.
     */
    public void start() throws Exception {
        Preconditions.checkState(this.tmpDir.get() != null, "Not Initialized.");
        val s = new ZooKeeperServer(this.tmpDir.get(), this.tmpDir.get(), ZooKeeperServer.DEFAULT_TICK_TIME);
        if (!this.server.compareAndSet(null, s)) {
            s.shutdown();
            throw new IllegalStateException("Already started.");
        }

        this.serverFactory.set(new NIOServerCnxnFactory());
        val address = LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort;
        log.info("Starting Zookeeper server at " + address + " ...");
        this.serverFactory.get().configure(new InetSocketAddress(LOOPBACK_ADDRESS, this.zkPort), 1000);
        this.serverFactory.get().startup(s);

        if (!waitForServerUp(this.zkPort)) {
            throw new IllegalStateException("ZooKeeper server failed to start");
        }
    }

    public void stop() {
        try {
            NIOServerCnxnFactory sf = this.serverFactory.getAndSet(null);
            if (sf != null) {
                sf.closeAll();
                sf.shutdown();
            }
        } catch (Throwable e) {
            log.warn("Unable to cleanly shutdown ZooKeeper connection factory", e);
        }

        try {
            ZooKeeperServer zs = this.server.getAndSet(null);
            if (zs != null) {
                zs.shutdown();
                ZKDatabase zkDb = zs.getZKDatabase();
                if (zkDb != null) {
                    // make ZK server close its log files
                    zkDb.close();
                }
            }

        } catch (Throwable e) {
            log.warn("Unable to cleanly shutdown ZooKeeper server", e);
        }
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

    /**
     * Main method that can be used to start ZooKeeper out-of-process using BookKeeperServiceRunner.
     * This is used when invoking this class via ProcessStarter.
     *
     * @param args Args.
     * @throws Exception If an error occurred.
     */
    public static void main(String[] args) throws Exception {
        int zkPort;
        try {
            zkPort = Integer.parseInt(System.getProperty(PROPERTY_ZK_PORT));
        } catch (Exception ex) {
            System.out.println(String.format("Invalid or missing arguments (via system properties). Expected: %s(int). (%s)",
                    PROPERTY_ZK_PORT, ex.getMessage()));
            System.exit(-1);
            return;
        }
        @Cleanup
        ZooKeeperServiceRunner runner = new ZooKeeperServiceRunner(zkPort);
        runner.initialize();
        runner.start();
        Thread.sleep(Long.MAX_VALUE);
    }
}
