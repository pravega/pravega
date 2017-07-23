package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.lang.ProcessHelpers;
import io.pravega.segmentstore.server.host.ServiceStarter;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;

/**
 * Store adapter wrapping a real Pravega Client targeting a local cluster out-of-process.
 */
public class OutOfProcessAdapter extends ClientAdapterBase {
    //region Members

    private static final String LOG_ID = "OutOfProcessAdapter";
    private static final int PROCESS_SHUTDOWN_TIMEOUT_MILLIS = 10000;
    private static final int BOOKIE_COUNT = 1; // TODO: these should go in some sort of config.
    private static final int CONTROLLER_COUNT = 1;
    private static final int SEGMENT_STORE_COUNT = 1;

    private final ServiceBuilderConfig builderConfig;
    private final AtomicReference<Process> zooKeeperProcess;
    private final AtomicReference<Process> bookieProcess;
    private final List<Process> segmentStoreProcesses;
    private final List<Process> controllerProcesses;
    private final AtomicReference<File> segmentStoreConfigFile;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ClientAdapterBase class.
     *
     * @param testConfig    The TestConfig to use.
     * @param builderConfig SegmentStore Builder Configuration.
     * @param testExecutor  An ExecutorService used by the Test Application.
     */
    public OutOfProcessAdapter(TestConfig testConfig, ServiceBuilderConfig builderConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.builderConfig = Preconditions.checkNotNull(builderConfig, "builderConfig");
        this.zooKeeperProcess = new AtomicReference<>();
        this.bookieProcess = new AtomicReference<>();
        this.segmentStoreProcesses = Collections.synchronizedList(new ArrayList<>());
        this.controllerProcesses = Collections.synchronizedList(new ArrayList<>());
        this.segmentStoreConfigFile = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();
        stopRunningProcesses();
        TestLogger.log(LOG_ID, "Closed.");
    }

    //endregion

    @Override
    public void initialize() throws Exception {
        try {
            startZooKeeper();
            startBookKeeper();
            startAllControllers();
            startAllSegmentStores();
            Thread.sleep(50000); // TODO: remove
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                stopRunningProcesses();
            }

            throw ex;
        }

        TestLogger.log(LOG_ID, "Initialized.");
        super.initialize();
    }

    @Override
    protected StreamManager getStreamManager() {
        return null;
    }

    @Override
    protected ClientFactory getClientFactory() {
        return null;
    }

    //region Services Startup/Shutdown

    private void startZooKeeper() throws IOException {
        Preconditions.checkState(this.zooKeeperProcess.get() == null, "ZooKeeper is already started.");
        this.zooKeeperProcess.set(ZooKeeperServiceRunner.startOutOfProcess(this.testConfig.getZkPort()));
        if (!ZooKeeperServiceRunner.waitForServerUp(this.testConfig.getZkPort())) {
            throw new RuntimeException("Unable to start ZooKeeper at port " + this.testConfig.getZkPort());
        }

        TestLogger.log(LOG_ID, "ZooKeeper started (Port = %s).", this.testConfig.getZkPort());
    }

    private void startBookKeeper() throws IOException {
        Preconditions.checkState(this.bookieProcess.get() == null, "Bookies are already started.");
        this.bookieProcess.set(BookKeeperServiceRunner.startOutOfProcess(
                this.testConfig.getBkPort(), BOOKIE_COUNT, this.testConfig.getZkPort(), SegmentStoreAdapter.BK_LEDGER_PATH));
        TestLogger.log(LOG_ID, "Bookies started (Count = %s, Ports = [%s-%s])",
                BOOKIE_COUNT, this.testConfig.getBkPort(), this.testConfig.getBkPort() + BOOKIE_COUNT - 1);
    }

    private void startAllControllers() throws IOException {
        Preconditions.checkState(this.controllerProcesses.size() == 0, "At least one Controller is already started.");
        val props = new HashMap<String, String>();
        props.put("config.controller.server.zk.url", String.format("localhost:%d", this.testConfig.getZkPort()));
        props.put("config.controller.server.store.host.type", "Zookeeper");
        for (int i = 0; i < CONTROLLER_COUNT; i++) {
            this.controllerProcesses.add(ProcessHelpers.exec(io.pravega.controller.server.Main.class, null, props));
            TestLogger.log(LOG_ID, "Controller %d/%d started", i, CONTROLLER_COUNT);
        }
    }

    private void startAllSegmentStores() throws IOException {
        Preconditions.checkState(this.segmentStoreProcesses.size() == 0, "At least one SegmentStore is already started.");
        createSegmentStoreConfigFile();
        // TODO: this may need more config values setup. Seems like some connections cannot be established.
        // * StorageImplementation & Setup (FileSystem)
        // * autoScale.controllerUri
        val props = Collections.singletonMap(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, this.segmentStoreConfigFile.get().getAbsolutePath());
        for (int i = 0; i < SEGMENT_STORE_COUNT; i++) {
            this.segmentStoreProcesses.add(ProcessHelpers.exec(ServiceStarter.class, null, props));
            TestLogger.log(LOG_ID, "SegmentStore %d/%d started", i, SEGMENT_STORE_COUNT);
        }
    }

    private void createSegmentStoreConfigFile() throws IOException {
        File f = this.segmentStoreConfigFile.get();
        if (f == null || !f.exists()) {
            f = File.createTempFile("selftest.segmentstore", "");
            f.deleteOnExit();
            this.segmentStoreConfigFile.set(f);
        }

        this.builderConfig.store(f);

    }

    private void stopRunningProcesses() {
        int controllerCount = stopAllProcesses(this.controllerProcesses);
        TestLogger.log(LOG_ID, "Controller(s) (%d count) shut down.", controllerCount);
        int segmentStoreCount = stopAllProcesses(this.segmentStoreProcesses);
        TestLogger.log(LOG_ID, "SegmentStore(s) (%d count) shut down.", segmentStoreCount);
        stopProcess(this.bookieProcess);
        TestLogger.log(LOG_ID, "Bookies shut down.");
        stopProcess(this.zooKeeperProcess);
        TestLogger.log(LOG_ID, "ZooKeeper shut down.");
    }

    private void stopProcess(AtomicReference<Process> processReference) {
        Process p = processReference.getAndSet(null);
        if (p != null) {
            p.destroy();
            Exceptions.handleInterrupted(() -> p.waitFor(PROCESS_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        }
    }

    private int stopAllProcesses(Collection<Process> processList) {
        processList.stream().filter(Objects::nonNull).forEach(p -> {
            p.destroy();
            Exceptions.handleInterrupted(() -> p.waitFor(PROCESS_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        });

        int count = processList.size();
        processList.clear();
        return count;
    }

    //endregion
}
