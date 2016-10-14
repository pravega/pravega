/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Main Entry Point for Pravega Segment Store Self Tester.
 */
class SelfTest extends AbstractService implements AutoCloseable {
    //region Members

    private static final String LOG_ID = "SelfTest";
    private final TestState state;
    private final TestConfig testConfig;
    private final AtomicBoolean closed;
    private final ScheduledExecutorService executor;
    private final ArrayList<Actor> actors;
    private final ProducerDataSource dataSource;
    private final AtomicReference<CompletableFuture<Void>> testCompletion;
    private final StoreAdapter store;
    private ServiceManager actorManager;

    //endregion

    //region Constructor

    SelfTest(TestConfig testConfig, ServiceBuilderConfig builderConfig) {
        Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(builderConfig, "builderConfig");

        this.testConfig = testConfig;
        this.state = new TestState();
        this.closed = new AtomicBoolean();
        this.actors = new ArrayList<>();
        //this.store = new ConsoleStoreAdapter();
        this.store = new StreamSegmentStoreAdapter(builderConfig);
        this.dataSource = new ProducerDataSource(this.testConfig, this.state, this.store);
        this.testCompletion = new AtomicReference<>();
        this.executor = Executors.newScheduledThreadPool(testConfig.getThreadPoolSize());
        addListener(new ServiceShutdownListener(this::shutdownCallback, this::shutdownCallback), this.executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);
            this.dataSource.deleteAllSegments().join();
            this.store.close();
            this.executor.shutdown();
            this.closed.set(true);
            TestLogger.log(LOG_ID, "Closed.");
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Initialize Test Completion Future.
        assert this.testCompletion.get() == null : "isRunning() == false, but testCompletion is not null";
        this.testCompletion.set(new CompletableFuture<>());

        // Create and initialize the Test Actors (Producers & Consumers).
        createTestActors();

        // Initialize Actor Manager and attach callbacks.
        this.actorManager = new ServiceManager(this.actors);
        this.actorManager.addListener(new ServiceManager.Listener() {
            @Override
            public void healthy() {
                // We are considered 'started' only after all TestActors are started.
                notifyStarted();
                TestLogger.log(LOG_ID, "Started.");
            }

            @Override
            public void stopped() {
                // We are considered 'stopped' only after all TestActors are stopped.
                notifyStopped();
            }

            @Override
            public void failure(Service service) {
                // We are considered 'failed' if at least one Actor failed.
                notifyFailed(service.failureCause());
            }
        }, this.executor);

        TestLogger.log(LOG_ID, "Starting.");

        // Create all segments, then start the Actor Manager.
        CompletableFuture<Void> startFuture =
                this.store.initialize(this.testConfig.getTimeout())
                          .thenCompose(v -> this.dataSource.createSegments())
                          .thenRunAsync(this.actorManager::startAsync, this.executor);
        FutureHelpers.exceptionListener(startFuture, this::notifyFailed);
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.actorManager.stopAsync();
    }

    //endregion

    /**
     * Waits for the SelfTest to complete.
     *
     * @return A CompletableFuture that will be completed when the SelfTest is finished. If the test failed, the Future
     * will be completed with the appropriate exception.
     */
    CompletableFuture<Void> awaitFinished() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(isRunning(), "SelfTest is not running.");
        CompletableFuture<Void> tc = this.testCompletion.get();
        assert tc != null : "isRunning() == true but testCompletion is not set";
        return tc;
    }

    private void createTestActors() {
        for (int i = 0; i < this.testConfig.getProducerCount(); i++) {
            Producer p = new Producer(Integer.toString(i), this.testConfig, this.dataSource, this.store, this.executor);
            this.actors.add(p);
        }
    }

    private void shutdownCallback() {
        // Same as shutdownCallback(Throwable), but don't pass an exception.
        shutdownCallback(null);
    }

    private void shutdownCallback(Throwable failureCause) {
        // Close all TestActors.
        this.actors.forEach(Actor::close);
        this.actors.clear();

        // Complete Test Completion Future
        if (failureCause == null) {
            TestLogger.log(LOG_ID, "Finished successfully.");
            this.testCompletion.get().complete(null);
        } else {
            TestLogger.log(LOG_ID, "Failed with error %s.", failureCause);
            this.testCompletion.get().completeExceptionally(failureCause);
        }

        this.actorManager = null;
    }
}
