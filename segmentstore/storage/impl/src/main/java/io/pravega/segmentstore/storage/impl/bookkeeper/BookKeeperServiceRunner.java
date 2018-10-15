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
import io.pravega.common.auth.JKSHelper;
import io.pravega.common.auth.ZKTLSUtils;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.Cleanup;
import lombok.SneakyThrows;
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

    public static final String PROPERTY_BASE_PORT = "basePort";
    public static final String PROPERTY_BOOKIE_COUNT = "bookieCount";
    public static final String PROPERTY_ZK_PORT = "zkPort";
    public static final String PROPERTY_LEDGERS_PATH = "ledgersPath";
    public static final String PROPERTY_START_ZK = "startZk";
    public static final String PROPERTY_SECURE_BK = "secureBk";
    public static final String TLS_KEY_STORE_PASSWD = "tlsKeyStorePasswd";
    public static final String TLS_KEY_STORE = "tlsKeyStore";

    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private final boolean startZk;
    private final int zkPort;
    private final String ledgersPath;
    private final boolean secureBK;
    private final String tLSKeyStore;
    private final String tLSKeyStorePasswordPath;
    private final boolean secureZK;
    private final String tlsTrustStore;
    private final List<Integer> bookiePorts;
    private final List<BookieServer> servers = new ArrayList<>();
    private final AtomicReference<ZooKeeperServiceRunner> zkServer = new AtomicReference<>();
    private final HashMap<Integer, File> tempDirs = new HashMap<>();
    private final AtomicReference<Thread> cleanup = new AtomicReference<>();

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {
        try {
            this.servers.stream().filter(Objects::nonNull).forEach(BookieServer::shutdown);
            if (this.zkServer.get() != null) {
                this.zkServer.get().close();
            }
        } finally {
            cleanupDirectories();
        }

        Thread c = this.cleanup.getAndSet(null);
        if (c != null) {
            Runtime.getRuntime().removeShutdownHook(c);
        }
    }

    @SneakyThrows
    private void cleanupOnShutdown() {
        close();
    }

    //endregion

    //region BookKeeper operations

    /**
     * Stops the BookieService with the given index.
     *
     * @param bookieIndex The index of the bookie to stop.
     */
    public void stopBookie(int bookieIndex) {
        Preconditions.checkState(this.servers.size() > 0, "No Bookies initialized. Call startAll().");
        Preconditions.checkState(this.servers.get(0) != null, "Bookie already stopped.");
        val bk = this.servers.get(bookieIndex);
        bk.shutdown();
        this.servers.set(bookieIndex, null);
        log.info("Bookie {} stopped.", bookieIndex);
    }

    /**
     * Restarts the BookieService with the given index.
     *
     * @param bookieIndex The index of the bookie to restart.
     * @throws Exception If an exception occurred.
     */
    public void startBookie(int bookieIndex) throws Exception {
        Preconditions.checkState(this.servers.size() > 0, "No Bookies initialized. Call startAll().");
        Preconditions.checkState(this.servers.get(0) == null, "Bookie already running.");
        this.servers.set(bookieIndex, runBookie(this.bookiePorts.get(bookieIndex)));
        log.info("Bookie {} stopped.", bookieIndex);
    }

    /**
     * Suspends (stops) ZooKeeper, without destroying its underlying data (so a subsequent resume can pick up from the
     * state where it left off).
     */
    public void suspendZooKeeper() {
        val zk = this.zkServer.get();
        Preconditions.checkState(zk != null, "ZooKeeper not started.");

        // Stop, but do not close, the ZK runner.
        zk.stop();
        log.info("ZooKeeper suspended.");
    }

    /**
     * Resumes ZooKeeper (if it had previously been suspended).
     *
     * @throws Exception If an exception got thrown.
     */
    public void resumeZooKeeper() throws Exception {
        val zk = new ZooKeeperServiceRunner(this.zkPort, this.secureZK, this.tLSKeyStore, this.tLSKeyStorePasswordPath, this.tlsTrustStore);
        if (this.zkServer.compareAndSet(null, zk)) {
            // Initialize ZK runner (since nobody else did it for us).
            zk.initialize();
            log.info("ZooKeeper initialized.");
        } else {
            zk.close();
        }

        // Start or resume ZK.
        this.zkServer.get().start();
        log.info("ZooKeeper resumed.");
    }

    /**
     * Starts the BookKeeper cluster in-process.
     *
     * @throws Exception If an exception occurred.
     */
    public void startAll() throws Exception {
        // Make sure the child processes and any created files get killed/deleted if the process is terminated.
        this.cleanup.set(new Thread(this::cleanupOnShutdown));
        Runtime.getRuntime().addShutdownHook(this.cleanup.get());

        if (this.startZk) {
            resumeZooKeeper();
        }

        initializeZookeeper();
        runBookies();
    }

    private void initializeZookeeper() throws Exception {
        log.info("Formatting ZooKeeper ...");

        if (this.secureZK) {
            ZKTLSUtils.setSecureZKClientProperties(this.tlsTrustStore, JKSHelper.loadPasswordFrom(this.tLSKeyStorePasswordPath));
        }

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

    private void runBookies() throws Exception {
        log.info("Starting Bookie(s) ...");
        for (int bkPort : this.bookiePorts) {
            this.servers.add(runBookie(bkPort));
        }
    }

    private BookieServer runBookie(int bkPort) throws Exception {
        // Attempt to reuse an existing data directory. This is useful in case of stops & restarts, when we want to perserve
        // already committed data.
        File tmpDir = this.tempDirs.getOrDefault(bkPort, null);
        if (tmpDir == null) {
            tmpDir = IOUtils.createTempDir("bookie_" + bkPort, "test");
            tmpDir.deleteOnExit();
            this.tempDirs.put(bkPort, tmpDir);
            log.info("Created " + tmpDir);
            if (!tmpDir.delete() || !tmpDir.mkdir()) {
                throw new IOException("Couldn't create bookie dir " + tmpDir);
            }
        }

        val conf = new ServerConfiguration();
        conf.setBookiePort(bkPort);
        conf.setZkServers(LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort);
        conf.setJournalDirName(tmpDir.getPath());
        conf.setLedgerDirNames(new String[]{tmpDir.getPath()});
        conf.setAllowLoopback(true);
        conf.setJournalAdaptiveGroupWrites(false);
        conf.setZkLedgersRootPath(ledgersPath);

        if (secureBK) {
            conf.setTLSProvider("OpenSSL");
            conf.setTLSProviderFactoryClass("org.apache.bookkeeper.tls.TLSContextFactory");
            conf.setTLSKeyStore(this.tLSKeyStore);
            conf.setTLSKeyStorePasswordPath(this.tLSKeyStorePasswordPath);
        }

        log.info("Starting Bookie at port " + bkPort);
        val bs = new BookieServer(conf);
        bs.start();
        return bs;
    }

    private void cleanupDirectories() throws IOException {
        for (File dir : this.tempDirs.values()) {
            log.info("Cleaning up " + dir);
            FileUtils.deleteDirectory(dir);
        }

        this.tempDirs.clear();
    }

    //endregion

    /**
     * Main method that can be used to start BookKeeper out-of-process using BookKeeperServiceRunner.
     * This is used when invoking this class via ProcessStarter.
     *
     * @param args Args.
     * @throws Exception If an error occurred.
     */
    public static void main(String[] args) throws Exception {
        val b = BookKeeperServiceRunner.builder();
        b.startZk(false);
        try {
            int bkBasePort = Integer.parseInt(System.getProperty(PROPERTY_BASE_PORT));
            int bkCount = Integer.parseInt(System.getProperty(PROPERTY_BOOKIE_COUNT));
            val bkPorts = new ArrayList<Integer>();
            for (int i = 0; i < bkCount; i++) {
                bkPorts.add(bkBasePort + i);
            }

            b.bookiePorts(bkPorts);
            b.zkPort(Integer.parseInt(System.getProperty(PROPERTY_ZK_PORT)));
            b.ledgersPath(System.getProperty(PROPERTY_LEDGERS_PATH));
            b.startZk(Boolean.parseBoolean(System.getProperty(PROPERTY_START_ZK, "false")));
            b.tLSKeyStore(System.getProperty(TLS_KEY_STORE, "../../../config/bookie.keystore.jks"));
            b.tLSKeyStorePasswordPath(System.getProperty(TLS_KEY_STORE_PASSWD, "../../../config/bookie.keystore.jks.passwd"));
            b.secureBK(Boolean.parseBoolean(System.getProperty(PROPERTY_SECURE_BK, "false")));

        } catch (Exception ex) {
            System.out.println(String.format("Invalid or missing arguments (via system properties). Expected: %s(int), " +
                            "%s(int), %s(int), %s(String). (%s).", PROPERTY_BASE_PORT, PROPERTY_BOOKIE_COUNT, PROPERTY_ZK_PORT,
                    PROPERTY_LEDGERS_PATH, ex.getMessage()));
            System.exit(-1);
            return;
        }

        BookKeeperServiceRunner runner = b.build();
        runner.startAll();
        Thread.sleep(Long.MAX_VALUE);
    }
}
