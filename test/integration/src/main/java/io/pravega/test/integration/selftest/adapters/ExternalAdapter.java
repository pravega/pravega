/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import io.pravega.test.integration.selftest.TestConfig;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;

/**
 * StoreAdapter for tests that target a Pravega Cluster out-of-process. This uses a real Client, Controller and StreamManager.
 * This class does not create or destroy Pravega Clusters; it just provides a means of accessing them. Derived classes
 * may choose to start such clusters or connect to existing ones.
 */
class ExternalAdapter extends ClientAdapterBase {
    //region Members

    private final AtomicReference<StreamManager> streamManager;
    private final AtomicReference<ClientFactory> clientFactory;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ExternalAdapter class.
     *
     * @param testConfig   The TestConfig to use.
     * @param testExecutor An Executor to use for async client-side operations.
     */
    ExternalAdapter(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.streamManager = new AtomicReference<>();
        this.clientFactory = new AtomicReference<>();
    }

    //endregion

    //region ClientAdapterBase and StorageAdapter Implementation

    @Override
    protected void startUp() throws Exception {
        try {
            URI controllerUri = new URI(getControllerUrl());

            // Create Stream Manager, Scope and Client Factory.
            this.streamManager.set(StreamManager.create(controllerUri));
            Retry.withExpBackoff(500, 2, 10)
                 .retryWhen(ex -> true)
                 .throwingOn(Exception.class)
                 .run(() -> this.streamManager.get().createScope(SCOPE));

            // Create Client Factory.
            this.clientFactory.set(ClientFactory.withScope(SCOPE, controllerUri));

            // Create, Seal and Delete a dummy segment - this verifies that the client is properly setup and that all the
            // components are running properly.
            String testStreamName = "Ping" + Long.toHexString(System.currentTimeMillis());
            this.streamManager.get().createStream(SCOPE, testStreamName, StreamConfiguration.builder().build());
            this.streamManager.get().sealStream(SCOPE, testStreamName);
            this.streamManager.get().deleteStream(SCOPE, testStreamName);
            log("Client initialized; using scope '%s'.", SCOPE);
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                close();
            }

            throw ex;
        }

        super.startUp();
    }

    @Override
    protected void shutDown() {
        super.shutDown();

        // Stop clients.
        stopComponent(this.clientFactory);
        stopComponent(this.streamManager);
    }

    @SneakyThrows(Exception.class)
    private void stopComponent(AtomicReference<? extends AutoCloseable> componentReference) {
        AutoCloseable p = componentReference.getAndSet(null);
        if (p != null) {
            p.close();
        }
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        // Even though it does support it, Feature.RandomRead is not enabled because it currently has very poor performance.
        return feature == Feature.Create
                || feature == Feature.Append
                || feature == Feature.TailRead
                || feature == Feature.Transaction
                || feature == Feature.Seal
                || feature == Feature.Delete;
    }

    @Override
    protected StreamManager getStreamManager() {
        return this.streamManager.get();
    }

    @Override
    protected ClientFactory getClientFactory() {
        return this.clientFactory.get();
    }

    @Override
    protected String getControllerUrl() {
        return String.format("tcp://%s:%d", this.testConfig.getControllerHost(), this.testConfig.getControllerPort(0));
    }

    //endregion

}
