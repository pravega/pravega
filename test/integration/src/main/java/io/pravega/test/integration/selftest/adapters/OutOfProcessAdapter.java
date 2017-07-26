/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.lang.ProcessStarter;
import io.pravega.common.util.Property;
import io.pravega.common.util.Retry;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;
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
    private final AtomicReference<Process> zooKeeperProcess;
    private final AtomicReference<Process> bookieProcess;
    private final List<Process> segmentStoreProcesses;
    private final List<Process> controllerProcesses;
    private final AtomicReference<File> segmentStoreConfigFile;
    private final AtomicReference<File> storageRoot;
    private final AtomicReference<StreamManager> streamManager;
    private final AtomicReference<ClientFactory> clientFactory;

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
        this.streamManager = new AtomicReference<>();
        this.clientFactory = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        super.close();

        // Stop clients.
        stopComponent(this.clientFactory);
        stopComponent(this.streamManager);

        // Stop all services.
        int controllerCount = stopProcesses(this.controllerProcesses);
        TestLogger.log(LOG_ID, "Controller(s) (%d count) shut down.", controllerCount);
        int segmentStoreCount = stopProcesses(this.segmentStoreProcesses);
        TestLogger.log(LOG_ID, "SegmentStore(s) (%d count) shut down.", segmentStoreCount);
        stopProcess(this.bookieProcess);
        TestLogger.log(LOG_ID, "Bookies shut down.");
        stopProcess(this.zooKeeperProcess);
        TestLogger.log(LOG_ID, "ZooKeeper shut down.");

        // Delete temporary files and directories.
        delete(this.segmentStoreConfigFile);
        delete(this.storageRoot);
        TestLogger.log(LOG_ID, "Closed.");
    }

    //endregion

    //region ClientAdapterBase and StorageAdapter Implementation

    @Override
    public boolean isFeatureSupported(Feature feature) {
        return feature == Feature.Create
                || feature == Feature.Append;
    }

    @Override
    public void initialize() throws Exception {
        try {
            createSegmentStoreFileSystem();
            startZooKeeper();
            startBookKeeper();
            startAllControllers();
            Thread.sleep(1000); // TODO: figure out how to remove.
            startAllSegmentStores();
            Thread.sleep(1000); // TODO: figure out how to remove.
            initializeClient();
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
        return this.streamManager.get();
    }

    @Override
    protected ClientFactory getClientFactory() {
        return this.clientFactory.get();
    }

    //endregion

    //region Services Startup/Shutdown

    @SneakyThrows(Exception.class)
    private void initializeClient() {
        Preconditions.checkState(this.streamManager.get() == null && this.clientFactory.get() == null, "Client is already initialized.");
        URI controllerUri = new URI(getControllerUrl());

        // Create Stream Manager, Scope and Client Factory.
        this.streamManager.set(StreamManager.create(controllerUri));
        Retry.withExpBackoff(500, 2, 10)
             .retryWhen(ex -> true)
             .throwingOn(Exception.class)
             .run(() -> this.streamManager.get().createScope(SCOPE));

        // Create Client Factory.
        this.clientFactory.set(ClientFactory.withScope(SCOPE, controllerUri));
        TestLogger.log(LOG_ID, "Scope '%s' created.", SCOPE);
    }

    private void startZooKeeper() throws Exception {
        Preconditions.checkState(this.zooKeeperProcess.get() == null, "ZooKeeper is already started.");
        this.zooKeeperProcess.set(ProcessStarter
                .forClass(ZooKeeperServiceRunner.class)
                .sysProp(ZooKeeperServiceRunner.PROPERTY_ZK_PORT, this.testConfig.getZkPort())
                .stdOut(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentOutLogPath("zk", 0))))
                .stdErr(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentErrLogPath("zk", 0))))
                .start());

        if (!ZooKeeperServiceRunner.waitForServerUp(this.testConfig.getZkPort())) {
            throw new RuntimeException("Unable to start ZooKeeper at port " + this.testConfig.getZkPort());
        }

        TestLogger.log(LOG_ID, "ZooKeeper started (Port = %s).", this.testConfig.getZkPort());
    }

    private void startBookKeeper() throws IOException {
        Preconditions.checkState(this.bookieProcess.get() == null, "Bookies are already started.");
        int bookieCount = this.testConfig.getBookieCount();
        this.bookieProcess.set(ProcessStarter
                .forClass(BookKeeperServiceRunner.class)
                .sysProp(BookKeeperServiceRunner.PROPERTY_BASE_PORT, this.testConfig.getBkPort())
                .sysProp(BookKeeperServiceRunner.PROPERTY_BOOKIE_COUNT, bookieCount)
                .sysProp(BookKeeperServiceRunner.PROPERTY_ZK_PORT, this.testConfig.getZkPort())
                .sysProp(BookKeeperServiceRunner.PROPERTY_LEDGERS_PATH, SegmentStoreAdapter.BK_LEDGER_PATH)
                .stdOut(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentOutLogPath("bk", 0))))
                .stdErr(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentErrLogPath("bk", 0))))
                .start());
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
        Process p = ProcessStarter
                .forClass(io.pravega.controller.server.Main.class)
                .sysProp("CONTAINER_COUNT", this.testConfig.getContainerCount())
                .sysProp("ZK_URL", getZkUrl())
                .sysProp("CONTROLLER_SERVER_PORT", port)
                .sysProp("REST_SERVER_IP", LOOPBACK_ADDRESS.getHostName())
                .sysProp("REST_SERVER_PORT", restPort)
                .sysProp("CONTROLLER_RPC_PUBLISHED_HOST", LOOPBACK_ADDRESS.getHostName())
                .sysProp("CONTROLLER_RPC_PUBLISHED_PORT", rpcPort)
                .stdOut(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentOutLogPath("controller", controllerId))))
                .stdErr(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentErrLogPath("controller", controllerId))))
                .start();
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
        Process p = ProcessStarter
                .forClass(ServiceStarter.class)
                .sysProp(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, this.segmentStoreConfigFile.get().getAbsolutePath())
                .sysProp(configProperty(ServiceConfig.COMPONENT_CODE, ServiceConfig.ZK_URL), getZkUrl())
                .sysProp(configProperty(BookKeeperConfig.COMPONENT_CODE, BookKeeperConfig.ZK_ADDRESS), getZkUrl())
                .sysProp(configProperty(ServiceConfig.COMPONENT_CODE, ServiceConfig.LISTENING_PORT), port)
                .sysProp(configProperty(ServiceConfig.COMPONENT_CODE, ServiceConfig.STORAGE_IMPLEMENTATION), ServiceConfig.StorageTypes.FILESYSTEM)
                .sysProp(configProperty(FileSystemStorageConfig.COMPONENT_CODE, FileSystemStorageConfig.ROOT), this.storageRoot.get().getAbsolutePath())
                .sysProp(configProperty(AutoScalerConfig.COMPONENT_CODE, AutoScalerConfig.CONTROLLER_URI), getControllerUrl())
                .stdOut(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentOutLogPath("segmentStore", segmentStoreId))))
                .stdErr(ProcessBuilder.Redirect.to(new File(this.testConfig.getComponentErrLogPath("segmentStore", segmentStoreId))))
                .start();

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

    private String getControllerUrl() {
        return String.format("tcp://%s:%d", LOOPBACK_ADDRESS.getHostName(), this.testConfig.getControllerPort(0));
    }

    private String configProperty(String componentCode, Property<?> property) {
        return String.format("%s.%s", componentCode, property.getName());
    }

    //endregion
}
