/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.io.filesystem.FileOperations;
import io.pravega.common.security.JKSHelper;
import io.pravega.common.security.ZKTLSUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.bookie.LedgerStorage;
import org.apache.bookkeeper.bookie.UncleanShutdownDetectionImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
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
    public static final String PROPERTY_LEDGERS_PATH = "zkLedgersPath"; // ZK namespace path for ledger metadata.
    public static final String PROPERTY_START_ZK = "startZk";
    public static final String PROPERTY_SECURE_BK = "secureBk";
    public static final String TLS_KEY_STORE_PASSWD = "tlsKeyStorePasswd";
    public static final String TLS_KEY_STORE = "tlsKeyStore";
    public static final String PROPERTY_LEDGERS_DIR = "ledgersDir"; // File System path to store ledger data.

    private static final String LOOPBACK_ADDRESS = "127.0.0.1";
    private final boolean startZk;
    private final int zkPort;
    private final String ledgersPath;
    private final boolean secureBK;
    private final String tLSKeyStore;
    private final String tLSKeyStorePasswordPath;
    private final boolean secureZK;
    private final String tlsTrustStore;
    private final String ledgersDir;
    private final List<Integer> bookiePorts;
    private final List<BookieServerAndResources> servers = new ArrayList<>();
    private final AtomicReference<ZooKeeperServiceRunner> zkServer = new AtomicReference<>();
    private final HashMap<Integer, File> journalDirs = new HashMap<>();
    private final HashMap<Integer, File> ledgerDirs = new HashMap<>();
    private final AtomicReference<Thread> cleanup = new AtomicReference<>();

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {
        try {
            this.servers.stream().filter(Objects::nonNull).forEach(bookieAndResources -> {
                bookieAndResources.getBookieServer().shutdown();
                bookieAndResources.getResources().cleanup();
            });
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
        val bk = this.servers.get(bookieIndex).getBookieServer();
        bk.shutdown();
        log.info("Bookie {} is stopping.", bookieIndex);
        this.servers.set(bookieIndex, null);
        log.info("Bookie {} stopped.", bookieIndex);
    }

    /**
     * Suspends processing for the BookieService with the given index.
     *
     * @param bookieIndex The index of the bookie to stop.
     */
    public void suspendBookie(int bookieIndex) {
        Preconditions.checkState(this.servers.size() > 0, "No Bookies initialized. Call startAll().");
        Preconditions.checkState(this.servers.get(0) != null, "Bookie does not exists.");
        val bk = this.servers.get(bookieIndex).getBookieServer();
        log.info("Bookie {} is suspending processing.", bookieIndex);
        bk.suspendProcessing();
        log.info("Bookie {} suspended processing.", bookieIndex);
    }

    /**
     * Resumes processing for the BookieService with the given index.
     *
     * @param bookieIndex The index of the bookie to stop.
     */
    public void resumeBookie(int bookieIndex) {
        Preconditions.checkState(this.servers.size() > 0, "No Bookies initialized. Call startAll().");
        Preconditions.checkState(this.servers.get(0) != null, "Bookie does not exists.");
        val bk = this.servers.get(bookieIndex).getBookieServer();
        log.info("Bookie {} is resuming processing.", bookieIndex);
        bk.resumeProcessing();
        log.info("Bookie {} resumed processing.", bookieIndex);
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
        log.info("Bookie {} is starting.", bookieIndex);
        this.servers.set(bookieIndex, runBookie(this.bookiePorts.get(bookieIndex)));
        log.info("Bookie {} started.", bookieIndex);
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
        zk.start();
        ZooKeeperServiceRunner.waitForServerUp(this.zkPort);
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
        } else {
            ZKTLSUtils.unsetSecureZKClientProperties();
        }

        @Cleanup
        val zkc = ZooKeeperClient.newBuilder()
                                 .connectString(LOOPBACK_ADDRESS + ":" + this.zkPort)
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

    private BookieServerAndResources runBookie(int bkPort) throws Exception {
        // Attempt to reuse an existing data directory. This is useful in case of stops & restarts, when we want to preserve
        // already committed data.
        File journalDir = this.journalDirs.getOrDefault(bkPort, null);
        if (journalDir == null) {
            journalDir = IOUtils.createTempDir("bookiejournal_" + bkPort, "_test");
            log.info("Journal Dir[{}]: {}.", bkPort, journalDir.getPath());
            this.journalDirs.put(bkPort, journalDir);
            setupTempDir(journalDir);
        }

        File ledgerDir = this.ledgerDirs.getOrDefault(bkPort, null);
        if (ledgerDir == null) {
            ledgerDir = Strings.isNullOrEmpty(this.ledgersDir) ? null : new File(this.ledgersDir);
            ledgerDir = IOUtils.createTempDir("bookieledger_" + bkPort, "_test", ledgerDir);
            log.info("Ledgers Dir[{}]: {}.", bkPort, ledgerDir.getPath());
            this.ledgerDirs.put(bkPort, ledgerDir);
            setupTempDir(ledgerDir);
        }

        val conf = new ServerConfiguration();
        conf.setAdvertisedAddress(LOOPBACK_ADDRESS);
        conf.setBookiePort(bkPort);
        conf.setMetadataServiceUri("zk://" + LOOPBACK_ADDRESS + ":" + this.zkPort + ledgersPath);
        conf.setJournalDirName(journalDir.getPath());
        conf.setLedgerDirNames(new String[]{ledgerDir.getPath()});
        conf.setAllowLoopback(true);
        conf.setJournalAdaptiveGroupWrites(true);

        if (secureBK) {
            conf.setTLSProvider("OpenSSL");
            conf.setTLSProviderFactoryClass("org.apache.bookkeeper.tls.TLSContextFactory");
            conf.setTLSKeyStore(this.tLSKeyStore);
            conf.setTLSKeyStorePasswordPath(this.tLSKeyStorePasswordPath);
        }

        log.info("Starting Bookie at port " + bkPort);
        BookieResources resources = new ResourceBuilder(conf).build();
        Bookie bookie = createBookie(resources);
        val bs = new BookieServer(conf, bookie, org.apache.bookkeeper.stats.NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT,
                new UncleanShutdownDetectionImpl(resources.getLedgerDirsManager()));
        bs.start();
        return new BookieServerAndResources(bs, resources);
    }

    private Bookie createBookie(BookieResources resources) throws Exception {
        return new BookieImpl(resources.conf,
                resources.registrationManager,
                resources.storage,
                resources.diskChecker,
                resources.ledgerDirsManager,
                resources.indexDirsManager,
                NullStatsLogger.INSTANCE,
                UnpooledByteBufAllocator.DEFAULT,
                new SimpleBookieServiceInfoProvider(resources.conf));
    }

    private void setupTempDir(File dir) throws IOException {
        dir.deleteOnExit();
        log.info("Created " + dir);
        if (!dir.delete() || !dir.mkdir()) {
            throw new IOException("Couldn't create bookie dir " + dir);
        }
    }

    private void cleanupDirectories() {
        FileOperations.cleanupDirectories(this.ledgerDirs);
        FileOperations.cleanupDirectories(this.journalDirs);
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
            b.ledgersDir(System.getProperty(PROPERTY_LEDGERS_DIR, null)); // null will colocate ledgers and journal.

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

    @Data
    static class BookieServerAndResources {
        private final BookieServer bookieServer;
        private final BookieResources resources;
    }

    /**
     * Manages bookie resources including their cleanup.
     */
    @Getter
    static class BookieResources {
        private final ServerConfiguration conf;
        private final MetadataBookieDriver metadataDriver;
        private final  RegistrationManager registrationManager;
        private final LedgerManagerFactory ledgerManagerFactory;
        private final LedgerManager ledgerManager;
        private final LedgerStorage storage;
        private final DiskChecker diskChecker;
        private final LedgerDirsManager ledgerDirsManager;
        private final LedgerDirsManager indexDirsManager;

        BookieResources(ServerConfiguration conf,
                        MetadataBookieDriver metadataDriver,
                        RegistrationManager registrationManager,
                        LedgerManagerFactory ledgerManagerFactory,
                        LedgerManager ledgerManager,
                        LedgerStorage storage,
                        DiskChecker diskChecker,
                        LedgerDirsManager ledgerDirsManager,
                        LedgerDirsManager indexDirsManager) {
            this.conf = conf;
            this.metadataDriver = metadataDriver;
            this.registrationManager = registrationManager;
            this.ledgerManagerFactory = ledgerManagerFactory;
            this.ledgerManager = ledgerManager;
            this.storage = storage;
            this.diskChecker = diskChecker;
            this.ledgerDirsManager = ledgerDirsManager;
            this.indexDirsManager = indexDirsManager;
        }

        void cleanup() {
            try {
                ledgerManager.close();
            } catch (Exception e) {
                log.warn("Error shutting down ledger manager", e);
            }
            try {
                ledgerManagerFactory.close();
            } catch (Exception e) {
                log.warn("Error shutting down ledger manager factory", e);
            }
            registrationManager.close();
            try {
                metadataDriver.close();
            } catch (Exception e) {
                log.warn("Error shutting down metadata driver", e);
            }
        }
    }

    /**
     * Builder for Bookie resources.
     */
    static class ResourceBuilder {
        private final ServerConfiguration conf;
        private MetadataBookieDriver metadataBookieDriver;
        private RegistrationManager registrationManager;

        public ResourceBuilder(ServerConfiguration conf) {
            this.conf = conf;
        }

        BookieResources build() throws Exception {
            if (metadataBookieDriver == null) {
                if (conf.getMetadataServiceUri() == null) {
                    metadataBookieDriver = new NullMetadataBookieDriver();
                } else {
                    metadataBookieDriver = org.apache.bookkeeper.bookie.BookieResources.createMetadataDriver(conf, NullStatsLogger.INSTANCE);
                }
            }
            if (registrationManager == null) {
                registrationManager = metadataBookieDriver.createRegistrationManager();
            }
            LedgerManagerFactory ledgerManagerFactory = metadataBookieDriver.getLedgerManagerFactory();
            LedgerManager ledgerManager = ledgerManagerFactory.newLedgerManager();

            DiskChecker diskChecker = org.apache.bookkeeper.bookie.BookieResources.createDiskChecker(conf);
            LedgerDirsManager ledgerDirsManager = org.apache.bookkeeper.bookie.BookieResources.createLedgerDirsManager(
                    conf, diskChecker, NullStatsLogger.INSTANCE);
            LedgerDirsManager indexDirsManager = org.apache.bookkeeper.bookie.BookieResources.createIndexDirsManager(
                    conf, diskChecker, NullStatsLogger.INSTANCE, ledgerDirsManager);

            LedgerStorage storage = org.apache.bookkeeper.bookie.BookieResources.createLedgerStorage(
                    conf, ledgerManager, ledgerDirsManager, indexDirsManager,
                    NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

            return new BookieResources(conf,
                    metadataBookieDriver,
                    registrationManager,
                    ledgerManagerFactory,
                    ledgerManager,
                    storage,
                    diskChecker,
                    ledgerDirsManager,
                    indexDirsManager);
        }
    }
}
