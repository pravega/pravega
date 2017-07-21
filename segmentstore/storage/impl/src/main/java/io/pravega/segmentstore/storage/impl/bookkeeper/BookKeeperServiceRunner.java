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
import io.pravega.common.lang.ProcessHelpers;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

/**
 * Helper class that starts BookKeeper in-process.
 */
@Slf4j
@Builder
public class BookKeeperServiceRunner implements AutoCloseable {
    //region Members

    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private final boolean startZk;
    private final int zkPort;
    private final String ledgersPath;
    private final List<Integer> bookiePorts;
    private final List<BookieServer> servers = new ArrayList<>();
    private final AtomicReference<ZooKeeperServiceRunner> zkServer = new AtomicReference<>();
    private final List<File> tempDirs = new ArrayList<>();

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {
        try {
            for (BookieServer bs : this.servers) {
                bs.shutdown();
            }

            if (this.zkServer.get() != null) {
                this.zkServer.get().close();
            }
        } finally {
            cleanupDirectories();
        }
    }

    //endregion

    //region BookKeeper operations

    /**
     * Suspends the BookieService with the given index (does not stop it).
     *
     * @param bookieIndex The index of the bookie to stop.
     * @throws ArrayIndexOutOfBoundsException If bookieIndex is invalid.
     */
    public void suspendBookie(int bookieIndex) {
        Preconditions.checkState(this.servers.size() > 0, "No Bookies initialized. Call startAll().");
        val bk = this.servers.get(bookieIndex);
        bk.suspendProcessing();
    }

    /**
     * Resumes the BookieService with the given index.
     *
     * @param bookieIndex The index of the bookie to start.
     * @throws ArrayIndexOutOfBoundsException If bookieIndex is invalid.
     */
    public void resumeBookie(int bookieIndex) {
        Preconditions.checkState(this.servers.size() > 0, "No Bookies initialized. Call startAll().");
        val bk = this.servers.get(bookieIndex);
        bk.resumeProcessing();
    }

    /**
     * Starts the BookKeeper cluster in-process.
     *
     * @throws Exception If an exception occurred.
     */
    public void startAll() throws Exception {
        if (this.startZk) {
            val zk = new ZooKeeperServiceRunner(this.zkPort);
            zk.start();
            this.zkServer.set(zk);
        }

        initializeZookeeper();
        val baseConf = new ServerConfiguration();
        runBookies(baseConf);
    }

    private void initializeZookeeper() throws Exception {
        log.info("Formatting ZooKeeper ...");
        @Cleanup
        val zkc = ZooKeeperClient.newBuilder()
                                 .connectString(LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort)
                                 .sessionTimeoutMs(10000)
                                 .build();

        String znode;
        StringBuilder znodePath = new StringBuilder();
        for (String z : this.ledgersPath.split("/")) {
            znodePath.append(z);
            znode = znodePath.toString();
            if (!znode.isEmpty()) {
                zkc.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            znodePath.append("/");
        }

        znodePath.append("available");
        zkc.create(znodePath.toString(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void runBookies(ServerConfiguration baseConf) throws Exception {
        log.info("Starting Bookie(s) ...");
        // Create Bookie Servers (B1, B2, B3)
        for (int bkPort : this.bookiePorts) {
            val tmpDir = IOUtils.createTempDir("bookie_" + bkPort, "test");
            tmpDir.deleteOnExit();
            this.tempDirs.add(tmpDir);
            log.info("Created " + tmpDir);
            if (!tmpDir.delete() || !tmpDir.mkdir()) {
                throw new IOException("Couldn't create bookie dir " + tmpDir);
            }

            // override settings
            val conf = new ServerConfiguration(baseConf);
            conf.setBookiePort(bkPort);
            conf.setZkServers(LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort);
            conf.setJournalDirName(tmpDir.getPath());
            conf.setLedgerDirNames(new String[]{ tmpDir.getPath() });
            conf.setAllowLoopback(true);
            conf.setJournalAdaptiveGroupWrites(false);
            conf.setZkLedgersRootPath(ledgersPath);

            log.info("Starting Bookie at port " + bkPort);
            val bs = new BookieServer(conf);
            this.servers.add(bs);
            bs.start();
        }
    }

    private void cleanupDirectories() throws IOException {
        for (File dir : this.tempDirs) {
            log.info("Cleaning up " + dir);
            FileUtils.deleteDirectory(dir);
        }
    }

    //endregion

    /**
     * Starts a BookKeeper Service Cluster out of process.
     *
     * @param bkBasePort  The first port where to start the Bookie on.
     * @param bookieCount The number of Bookies to start. Each will get a port assigned incrementally starting at bkBasePort.
     * @param zkPort      The port where ZooKeeper is listening at.
     * @param ledgersPath Path in ZooKeeper where to store BookKeeper ledger metadata.
     * @return A Process reference.
     * @throws IOException If an exception occurred.
     */
    public static Process startOutOfProcess(int bkBasePort, int bookieCount, int zkPort, String ledgersPath) throws IOException {
        return ProcessHelpers.exec(BookKeeperServiceRunner.class, bkBasePort, bookieCount, zkPort, ledgersPath);
    }

    public static void main(String[] args) throws Exception {
        val b = BookKeeperServiceRunner.builder();
        b.startZk(false);
        try {
            int argIndex = 0;
            int bkBasePort = Integer.parseInt(args[argIndex++]);
            int bkCount = Integer.parseInt(args[argIndex++]);
            val bkPorts = new ArrayList<Integer>();
            for (int i = 0; i < bkCount; i++) {
                bkPorts.add(bkBasePort + i);
            }

            b.bookiePorts(bkPorts);
            b.zkPort(Integer.parseInt(args[argIndex++]));
            b.ledgersPath(args[argIndex]);
        } catch (Exception ex) {
            System.out.println(String.format("Invalid or missing arguments. Expected: [BKPort(int) ZKPort(int) LedgersPath(String)] (%s).", ex.getMessage()));
            System.exit(-1);
            return;
        }

        BookKeeperServiceRunner runner = b.build();
        runner.startAll();
        Thread.sleep(Long.MAX_VALUE);
    }
}
