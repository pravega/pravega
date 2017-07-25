package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.lang.ProcessHelpers;
import io.pravega.common.util.Property;
import io.pravega.segmentstore.server.host.ServiceStarter;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.bookkeeper.ZooKeeperServiceRunner;
import io.pravega.segmentstore.storage.impl.filesystem.FileSystemStorageConfig;
import io.pravega.test.integration.selftest.TestConfig;
import io.pravega.test.integration.selftest.TestLogger;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.bookkeeper.util.IOUtils;

/**
 * Store adapter wrapping a real Pravega Client targeting a local cluster out-of-process.
 */
public class OutOfProcessAdapter extends ClientAdapterBase {
    //region Members

    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private static final String LOG_ID = "OutOfProcessAdapter";
    private static final int PROCESS_SHUTDOWN_TIMEOUT_MILLIS = 10000;
    private final ServiceBuilderConfig builderConfig;
    private final AtomicReference<ZooKeeperServiceRunner> zooKeeperProcess;
    private final AtomicReference<Process> bookieProcess;
    private final List<Process> segmentStoreProcesses;
    private final List<Process> controllerProcesses;
    private final AtomicReference<File> segmentStoreConfigFile;
    private final AtomicReference<File> storageRoot;

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
        Preconditions.checkArgument(testConfig.getBookieCount() > 0, "OutOfProcessAdapter requires at least one Bookie.");
        this.builderConfig = Preconditions.checkNotNull(builderConfig, "builderConfig");
        this.zooKeeperProcess = new AtomicReference<>();
        this.bookieProcess = new AtomicReference<>();
        this.segmentStoreProcesses = Collections.synchronizedList(new ArrayList<>());
        this.controllerProcesses = Collections.synchronizedList(new ArrayList<>());
        this.segmentStoreConfigFile = new AtomicReference<>();
        this.storageRoot = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();

        // Stop all services.
        int controllerCount = stopProcesses(this.controllerProcesses);
        TestLogger.log(LOG_ID, "Controller(s) (%d count) shut down.", controllerCount);
        int segmentStoreCount = stopProcesses(this.segmentStoreProcesses);
        TestLogger.log(LOG_ID, "SegmentStore(s) (%d count) shut down.", segmentStoreCount);
        stopProcess(this.bookieProcess);
        TestLogger.log(LOG_ID, "Bookies shut down.");
        stopComponent(this.zooKeeperProcess);
        TestLogger.log(LOG_ID, "ZooKeeper shut down.");

        // Delete temporary files and directories.
        delete(this.segmentStoreConfigFile);
        delete(this.storageRoot);
        TestLogger.log(LOG_ID, "Closed.");
    }

    //endregion

    @Override
    public void initialize() throws Exception {
        try {
            createSegmentStoreFileSystem();
            startZooKeeper();
            startBookKeeper();
            startAllControllers();
            Thread.sleep(10000); // TODO: remove
            startAllSegmentStores();
            Thread.sleep(200000); // TODO: remove
        } catch (Throwable ex) {
            if (!ExceptionHelpers.mustRethrow(ex)) {
                close();
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

    private void startZooKeeper() throws Exception {
        Preconditions.checkState(this.zooKeeperProcess.get() == null, "ZooKeeper is already started.");
        this.zooKeeperProcess.set(new ZooKeeperServiceRunner(this.testConfig.getZkPort()));
        this.zooKeeperProcess.get().start();
        if (!ZooKeeperServiceRunner.waitForServerUp(this.testConfig.getZkPort())) {
            throw new RuntimeException("Unable to start ZooKeeper at port " + this.testConfig.getZkPort());
        }

        TestLogger.log(LOG_ID, "ZooKeeper started (Port = %s).", this.testConfig.getZkPort());
    }

    private void startBookKeeper() throws IOException {
        Preconditions.checkState(this.bookieProcess.get() == null, "Bookies are already started.");
        int bookieCount = this.testConfig.getBookieCount();
        this.bookieProcess.set(BookKeeperServiceRunner.startOutOfProcess(
                this.testConfig.getBkPort(), bookieCount, this.testConfig.getZkPort(), SegmentStoreAdapter.BK_LEDGER_PATH));
        TestLogger.log(LOG_ID, "Bookies started (Count = %s, Ports = [%s-%s])",
                bookieCount, this.testConfig.getBkPort(0), this.testConfig.getBkPort(bookieCount - 1));
    }

    private void startAllControllers() throws IOException {
        Preconditions.checkState(this.controllerProcesses.size() == 0, "At least one Controller is already started.");
        for (int i = 0; i < this.testConfig.getControllerCount(); i++) {
            this.controllerProcesses.add(startController(i));
        }
    }

    private Process startController(int controllerId) throws IOException {
        int port = this.testConfig.getControllerPort(controllerId);
        int restPort = this.testConfig.getControllerRestPort(controllerId);
        int rpcPort = this.testConfig.getControllerRpcPort(controllerId);
        val props = ImmutableMap
                .<String, String>builder()
                .put("CONTAINER_COUNT", Integer.toString(this.testConfig.getContainerCount()))
                .put("ZK_URL", getZkUrl())
                .put("CONTROLLER_SERVER_PORT", Integer.toString(port))
                .put("REST_SERVER_IP", LOOPBACK_ADDRESS.getHostName())
                .put("REST_SERVER_PORT", Integer.toString(restPort))
                .put("CONTROLLER_RPC_PUBLISHED_HOST", LOOPBACK_ADDRESS.getHostName())
                .put("CONTROLLER_RPC_PUBLISHED_PORT", Integer.toString(rpcPort))
                .build();
        Process p = ProcessHelpers.exec(io.pravega.controller.server.Main.class, null, props);
        TestLogger.log(LOG_ID, "Controller %d started (Port = %d, RestPort = %d, RPCPort = %d).",
                controllerId, port, restPort, rpcPort);
        return p;
    }

    private void startAllSegmentStores() throws IOException {
        Preconditions.checkState(this.segmentStoreProcesses.size() == 0, "At least one SegmentStore is already started.");
        createSegmentStoreFileSystem();
        for (int i = 0; i < this.testConfig.getSegmentStoreCount(); i++) {
            this.segmentStoreProcesses.add(startSegmentStore(i));
        }
    }

    private Process startSegmentStore(int segmentStoreId) throws IOException {
        int port = this.testConfig.getSegmentStorePort(segmentStoreId);
        val props = ImmutableMap
                .<String, String>builder()
                .put(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, this.segmentStoreConfigFile.get().getAbsolutePath())
                .put(configProperty(ServiceConfig.COMPONENT_CODE, ServiceConfig.ZK_URL), getZkUrl())
                .put(configProperty(BookKeeperConfig.COMPONENT_CODE, BookKeeperConfig.ZK_ADDRESS), getZkUrl())
                .put(configProperty(ServiceConfig.COMPONENT_CODE, ServiceConfig.LISTENING_PORT), Integer.toString(port))
                .put(configProperty(ServiceConfig.COMPONENT_CODE, ServiceConfig.STORAGE_IMPLEMENTATION), ServiceConfig.StorageTypes.FILESYSTEM.toString())
                .put(configProperty(FileSystemStorageConfig.COMPONENT_CODE, FileSystemStorageConfig.ROOT), this.storageRoot.get().getAbsolutePath())
                .put(configProperty(AutoScalerConfig.COMPONENT_CODE, AutoScalerConfig.CONTROLLER_URI), getControllerRpcUrl())
                .build();

        Process p = ProcessHelpers.exec(ServiceStarter.class, null, props);
        TestLogger.log(LOG_ID, "SegmentStore %d started (Port = %d).", segmentStoreId, port);
        return p;
    }

    private void createSegmentStoreFileSystem() throws IOException {
        // Config file.
        File f = this.segmentStoreConfigFile.get();
        if (f == null || !f.exists()) {
            f = File.createTempFile("selftest.segmentstore", "");
            f.deleteOnExit();
            this.segmentStoreConfigFile.set(f);
        }

        // Tier2 Storage FileSystem root.
        File d = this.storageRoot.get();
        if (d == null || !d.exists()) {
            d = IOUtils.createTempDir("selftest.segmentstore", "storage");
            d.deleteOnExit();
            this.storageRoot.set(d);
        }

        this.builderConfig.store(f);
    }

    @SneakyThrows(Exception.class)
    private void stopComponent(AtomicReference<? extends AutoCloseable> componentReference) {
        AutoCloseable p = componentReference.getAndSet(null);
        if (p != null) {
            p.close();
        }
    }

    private void stopProcess(AtomicReference<Process> processReference) {
        Process p = processReference.getAndSet(null);
        if (p != null) {
            p.destroy();
            Exceptions.handleInterrupted(() -> p.waitFor(PROCESS_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        }
    }

    private int stopProcesses(Collection<Process> processList) {
        processList.stream().filter(Objects::nonNull).forEach(p -> {
            p.destroy();
            Exceptions.handleInterrupted(() -> p.waitFor(PROCESS_SHUTDOWN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        });

        int count = processList.size();
        processList.clear();
        return count;
    }

    private void delete(AtomicReference<File> fileRef) {
        File f = fileRef.getAndSet(null);
        if (f != null && f.exists()) {
            FileHelpers.deleteFileOrDirectory(f);
        }
    }

    private String getZkUrl() {
        return String.format("%s:%d", LOOPBACK_ADDRESS.getHostAddress(), this.testConfig.getZkPort());
    }

    private String getControllerRpcUrl() {
        return String.format("tcp://%s:%d", LOOPBACK_ADDRESS.getHostName(), this.testConfig.getControllerPort(0));
    }

    private String configProperty(String componentCode, Property<?> property) {
        return String.format("%s.%s", componentCode, property.getName());
    }

    //endregion
}
