package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.lang.ProcessHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
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
    private static final int CONTROLLER_COUNT = 3;
    private static final int SEGMENT_STORE_COUNT = 3;

    private final ServiceBuilderConfig builderConfig;
    private final AtomicReference<Process> zooKeeperProcess;
    private final AtomicReference<Process> bookieProcess;
    private final List<Process> segmentStoreProcesses;
    private final List<Process> controllerProcesses;

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
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();
        stopRunningProcesses();
        TestLogger.log(LOG_ID, "Closed.");
    }

    private void stopRunningProcesses() {
        stopAllProcesses(this.controllerProcesses);
        TestLogger.log(LOG_ID, "Controller(s) shut down.");
        stopAllProcesses(this.segmentStoreProcesses);
        TestLogger.log(LOG_ID, "SegmentStore(s) shut down.");
        stopProcess(this.bookieProcess);
        TestLogger.log(LOG_ID, "Bookie(s) shut down.");
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

    private void stopAllProcesses(Collection<Process> processList) {
        processList.stream().filter(Objects::nonNull).forEach(p -> {
            p.destroy();
            Exceptions.handleInterrupted(() -> p.waitFor(PROCESS_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        });
        processList.clear();
    }

    //endregion

    @Override
    public void initialize() throws Exception {
        try {
            // Start ZooKeeper
            this.zooKeeperProcess.set(ZooKeeperServiceRunner.startOutOfProcess(this.testConfig.getZkPort()));
            if (!ZooKeeperServiceRunner.waitForServerUp(this.testConfig.getZkPort())) {
                throw new RuntimeException("Unable to start ZooKeeper at port " + this.testConfig.getZkPort());
            }

            TestLogger.log(LOG_ID, "ZooKeeper started (Port = %s).", this.testConfig.getZkPort());

            // Start BookKeeper.
            this.bookieProcess.set(BookKeeperServiceRunner.startOutOfProcess(
                    this.testConfig.getBkPort(), 1, this.testConfig.getZkPort(), SegmentStoreAdapter.BK_LEDGER_PATH));
            TestLogger.log(LOG_ID, "Bookie(s) started (Count = %s, Ports = [%s-%s])",
                    BOOKIE_COUNT, this.testConfig.getBkPort(), this.testConfig.getBkPort() + BOOKIE_COUNT - 1);

            // Start Controller
            val controllerProps = new HashMap<String, String>();
            controllerProps.put("config.controller.server.zk.url", String.format("localhost:%d", this.testConfig.getZkPort()));
            controllerProps.put("config.controller.server.store.host.type", "Zookeeper");
            for (int i = 0; i < CONTROLLER_COUNT; i++) {
                this.controllerProcesses.add(ProcessHelpers.exec(io.pravega.controller.server.Main.class, null, controllerProps));
            }
            TestLogger.log(LOG_ID, "Controller(s) started (Count = %s)", CONTROLLER_COUNT);

            // Start SegmentStore
            // TODO: get ServiceBuilderConfig and write it to a temp file, then point ServiceStarter to that file.
            Thread.sleep(5000);

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
}
