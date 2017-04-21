/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

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
    private final List<Integer> bookiePorts;
    private final List<BookieServer> servers = new ArrayList<>();
    private final AtomicReference<ZooKeeperServer> zkServer = new AtomicReference<>();
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
                this.zkServer.get().shutdown();
            }
        } finally {
            cleanupDirectories();
        }
    }

    //endregion

    //region BookKeeper operations

    /**
     * Starts the BookKeeper cluster in-process.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        if (this.startZk) {
            runZookeeper();
        }

        initializeZookeeper();
        val baseConf = new ServerConfiguration();
        runBookies(baseConf);
    }

    private void runZookeeper() throws Exception {
        val tmpDir = IOUtils.createTempDir("zookeeper", "localbookkeeper");

        val zks = new ZooKeeperServer(tmpDir, tmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        this.zkServer.set(zks);
        val serverFactory = new NIOServerCnxnFactory();
        val address = LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort;
        log.info("Starting Zookeeper server at " + address + " ...");
        serverFactory.configure(new InetSocketAddress(LOOPBACK_ADDRESS, this.zkPort), 1000);
        serverFactory.startup(zks);

        boolean b = LocalBookKeeper.waitForServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT);
        log.info("ZooKeeper server {}.", b ? "up" : "not up");
    }

    private void initializeZookeeper() throws Exception {
        log.info("Formatting ZooKeeper ...");
        @Cleanup
        val zkc = ZooKeeperClient.newBuilder()
                                 .connectString(LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort)
                                 .sessionTimeoutMs(10000)
                                 .build();

        zkc.create("/ledgers", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    private void runBookies(ServerConfiguration baseConf) throws Exception {
        log.info("Starting Bookie(s) ...");
        // Create Bookie Servers (B1, B2, B3)
        for (int bkPort : this.bookiePorts) {
            val tmpDir = File.createTempFile("bookie_" + bkPort, "test");
            log.info("Created " + tmpDir);
            if (!tmpDir.delete() || !tmpDir.mkdir()) {
                throw new IOException("Couldn't create bookie dir " + tmpDir);
            }

            // override settings
            val conf = new ServerConfiguration(baseConf);
            conf.setBookiePort(bkPort);
            conf.setZkServers(LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort);
            conf.setJournalDirName(tmpDir.getPath());
            conf.setLedgerDirNames(new String[]{tmpDir.getPath()});
            conf.setAllowLoopback(true);
            conf.setJournalAdaptiveGroupWrites(false);

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
}
