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

import io.netty.buffer.UnpooledByteBufAllocator;
import io.pravega.common.io.filesystem.FileOperations;
import io.pravega.common.util.CommonUtils;
import lombok.val;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicReference;

public class BookKeeperServiceRunnerTest {

    HashMap<Integer, File> journalDirs = new HashMap<>();
    HashMap<Integer, File> ledgerDirs = new HashMap<>();
    AtomicReference<CuratorFramework> zkClient = new AtomicReference<>();
    AtomicReference<BookKeeperConfig> config = new AtomicReference<>();
    AtomicReference<BookKeeperLogFactory> factory = new AtomicReference<>();
    AtomicReference<BookKeeperServiceRunner> bkService = new AtomicReference<>();
    @Test
    public void testGetBookie() throws Exception {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(1);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        cleanupDirectories();
        String testId = Long.toHexString(System.nanoTime());
        int zkPort = CommonUtils.getAvailableListenPort();
        String ledgersPath = "/pravega/bookkeeper/ledgers/" + testId;
        int bkPort = CommonUtils.getAvailableListenPort();
        val bookiePorts = Arrays.asList(bkPort);

        File journalDir = journalDirs.getOrDefault(bkPort, null);
        if (journalDir == null) {
            journalDir = IOUtils.createTempDir("bookiejournal_" + bkPort, "_test");
            journalDirs.put(1, journalDir);
            setupTempDir(journalDir);
        }

        String ledgersDir = "ledgersDir";
        File ledgerDir = ledgerDirs.getOrDefault(bkPort, null);
        if (ledgerDir == null) {
            ledgerDir = new File(ledgersDir);
            if (!ledgerDir.exists()) {
                ledgerDir.mkdir();
            }
            ledgerDir = IOUtils.createTempDir("bookieledger_" + bkPort, "_test", ledgerDir);
            ledgerDirs.put(1, ledgerDir);
            setupTempDir(ledgerDir);
        }

        ServerConfiguration conf = new ServerConfiguration();
        conf.setAdvertisedAddress("127.0.0.1");
        conf.setBookiePort(bkPort);
        conf.setMetadataServiceUri("zk://127.0.0.1:" + zkPort + ledgersPath);
        conf.setJournalDirName(journalDir.getPath());
        conf.setLedgerDirNames(new String[]{ledgerDir.getPath()});
        conf.setAllowLoopback(true);
        conf.setJournalAdaptiveGroupWrites(true);
        conf.setAuthorizedRoles("test");

        //Mockito.when(conf.asJson()).thenThrow(new JsonUtil.ParseJsonException("Test"));
        val runner = BookKeeperServiceRunner.builder()
                .startZk(true)
                .zkPort(zkPort)
                .ledgersPath(ledgersPath)
                .secureBK(false)
                .secureZK(false)
                .tlsTrustStore("../../../config/bookie.truststore.jks")
                .tLSKeyStore("../../../config/bookie.keystore.jks")
                .tLSKeyStorePasswordPath("../../../config/bookie.keystore.jks.passwd")
                .bookiePorts(bookiePorts)
                .build();
        runner.startAll();
        bkService.set(runner);

        // Create a ZKClient with a unique namespace.
        String namespace = "pravega/segmentstore/unittest_" + testId;
        this.zkClient.set(CuratorFrameworkFactory
                .builder()
                .connectString("127.0.0.1:" + zkPort)
                .namespace(namespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 10))
                .build());
        this.zkClient.get().start();
        this.zkClient.get().blockUntilConnected();

        // Setup config to use the port and namespace.
        this.config.set(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "127.0.0.1:" + zkPort)
                .with(BookKeeperConfig.MAX_WRITE_ATTEMPTS, 5)
                .with(BookKeeperConfig.BK_LEDGER_MAX_SIZE,  200 * 25 )
                .with(BookKeeperConfig.BK_DIGEST_TYPE, DigestType.DUMMY.name())
                .with(BookKeeperConfig.ZK_METADATA_PATH, namespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, ledgersPath)
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, 1)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, 1)
                .with(BookKeeperConfig.BK_TLS_ENABLED, false)
                .with(BookKeeperConfig.BK_WRITE_TIMEOUT, 5000)
                .build());

        // Create default factory.
        val factory = new BookKeeperLogFactory(this.config.get(), this.zkClient.get(), es);
        factory.initialize();
        this.factory.set(factory);

        BookKeeperServiceRunner.BookieResources resources = new BookKeeperServiceRunner.ResourceBuilder(conf).build();
        Bookie bookie = createBookie(resources);

        conf.setPermittedStartupUsers("test");

        DiskChecker diskChecker = org.apache.bookkeeper.bookie.BookieResources.createDiskChecker(conf);
        LedgerDirsManager ledgerDirsManager = org.apache.bookkeeper.bookie.BookieResources.createLedgerDirsManager(
                conf, diskChecker, NullStatsLogger.INSTANCE);

        try {
            runner.getBookieServer(conf, ledgerDirsManager, bookie);
        } catch (Exception e) {
            System.out.println("Exceptionn occured");
            e.printStackTrace();
        }
    }

    private void cleanupDirectories() {
        FileOperations.cleanupDirectories(this.ledgerDirs);
        FileOperations.cleanupDirectories(this.journalDirs);
    }

    private Bookie createBookie(BookKeeperServiceRunner.BookieResources resources) {
        try {
            return new BookieImpl(resources.getConf(),
                    resources.getRegistrationManager(),
                    resources.getStorage(),
                    resources.getDiskChecker(),
                    resources.getLedgerDirsManager(),
                    resources.getIndexDirsManager(),
                    NullStatsLogger.INSTANCE,
                    UnpooledByteBufAllocator.DEFAULT,
                    new SimpleBookieServiceInfoProvider(resources.getConf()));
        } catch (IOException | InterruptedException | BookieException e) {
            throw new RuntimeException(e);
        }
    }

    private void setupTempDir(File dir) throws IOException {
        dir.deleteOnExit();
        if (!dir.delete() || !dir.mkdir()) {
            throw new IOException("Couldn't create bookie dir " + dir);
        }
    }
}
