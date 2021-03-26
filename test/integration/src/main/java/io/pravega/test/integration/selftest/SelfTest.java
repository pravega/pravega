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
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;

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
    private final Reporter reporter;
    private final ProducerDataSource<?> dataSource;
    private final AtomicReference<CompletableFuture<Void>> testCompletion;
    private final StoreAdapter store;
    private ServiceManager actorManager;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SelfTest class.
     *
     * @param testConfig    The configuration to use for the test.
     * @param builderConfig The configuration to use for building the StreamSegmentStore Service.
     */
    SelfTest(TestConfig testConfig, ServiceBuilderConfig builderConfig) {
        Preconditions.checkNotNull(builderConfig, "builderConfig");

        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.closed = new AtomicBoolean();
        this.actors = new ArrayList<>();
        this.testCompletion = new AtomicReference<>();
        this.state = new TestState();
        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(testConfig.getThreadPoolSize(), "self-test");
        this.store = StoreAdapter.create(testConfig, builderConfig, this.executor);
        this.dataSource = createProducerDataSource(this.testConfig, this.state, this.store);
        Services.onStop(this, this::shutdownCallback, this::shutdownCallback, this.executor);
        this.reporter = new Reporter(this.state, this.testConfig, this.store::getStorePoolSnapshot, this.executor);
    }

    private ProducerDataSource<?> createProducerDataSource(TestConfig config, TestState state, StoreAdapter store) {
        return config.getTestType().isTablesTest()
                ? new TableProducerDataSource(config, state, store)
                : new StreamProducerDataSource(config, state, store);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            try {
                Futures.await(Services.stopAsync(this, this.executor));
                this.dataSource.deleteAll()
                        .exceptionally(ex -> {
                            TestLogger.log(LOG_ID, "Unable to delete all Streams: %s.", ex);
                            return null;
                        }).join();

                this.store.close();
            } finally {
                ExecutorServiceHelpers.shutdown(this.executor);
                this.closed.set(true);
                TestLogger.log(LOG_ID, "Closed.");

            }
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

        TestLogger.log(LOG_ID, "Starting.");

        // Create all segments, then start the Actor Manager.
        Services.startAsync(this.store, this.executor)
                .thenCompose(v -> this.dataSource.createAll())
                .thenRunAsync(() -> {
                            // Create and initialize the Test Actors (Producers & Consumers).
                            createTestActors();

                            // Initialize Actor Manager and attach callbacks.
                            this.actorManager = new ServiceManager(this.actors);
                            this.actorManager.addListener(new ServiceManager.Listener() {
                                @Override
                                public void healthy() {
                                    // We are considered 'started' only after all Actors are started.
                                    notifyStarted();
                                    TestLogger.log(LOG_ID, "Started.");
                                }

                                @Override
                                public void stopped() {
                                    // We are considered 'stopped' only after all Actors are stopped.
                                    notifyStopped();
                                }

                                @Override
                                public void failure(Service service) {
                                    // We are considered 'failed' if at least one Actor failed.
                                    notifyFailed(service.failureCause());
                                }
                            }, this.executor);

                            this.actorManager.startAsync();
                            this.state.setWarmup(this.testConfig.getWarmupCount() > 0);
                            this.reporter.startAsync();
                        },
                        this.executor)
                .exceptionally(ex -> {
                    TestLogger.log(LOG_ID, "Startup failure: " + ex);
                    ex.printStackTrace(System.out);
                    notifyFailed(ex);
                    return null;
                });
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
        // Create Producers (based on TestConfig).
        for (int i = 0; i < this.testConfig.getProducerCount(); i++) {
            this.actors.add(new Producer<>(i, this.testConfig, this.dataSource, this.store, this.executor));
        }

        TestLogger.log(LOG_ID, "Created %s Producer(s).", this.testConfig.getProducerCount());

        // Create Consumers.
        if (!this.testConfig.isReadsEnabled()) {
            TestLogger.log(LOG_ID, "Not creating any consumers because reads are not enabled.");
            return;
        }

        if (this.testConfig.getTestType().isTablesTest()) {
            // Create TableConsumer.
            this.actors.add(new TableConsumer(this.testConfig, (TableProducerDataSource) this.dataSource, this.state, this.store, this.executor));
            TestLogger.log(LOG_ID, "Created TableConsumer.");
        } else {
            // Create Stream Consumers.
            if (!Consumer.canUseStoreAdapter(this.store)) {
                TestLogger.log(LOG_ID, "Not creating any consumers because the StorageAdapter does not support all required features.");
                return;
            }

            int count = 0;
            for (val si : this.state.getAllStreams()) {
                if (!si.isTransaction()) {
                    this.actors.add(new Consumer(si.getName(), this.testConfig, this.state, this.store, this.executor));
                    count++;
                }
            }

            TestLogger.log(LOG_ID, "Created %s Consumer(s).", count);
        }
    }

    private void shutdownCallback() {
        // Same as shutdownCallback(Throwable), but don't pass an exception.
        shutdownCallback(null);
    }

    private void shutdownCallback(Throwable failureCause) {
        // Stop reporting.
        this.reporter.stopAsync();

        // Close all TestActors.
        this.actors.forEach(Actor::close);
        this.actors.clear();

        // Output final state and summary, whether successful or not.
        this.reporter.outputState();
        this.reporter.outputSummary();

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
