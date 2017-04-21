/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.selftest;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.concurrent.ServiceShutdownListener;
import io.pravega.service.server.store.ServiceBuilderConfig;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.val;

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
    private final Reporter reporter;
    private final ProducerDataSource dataSource;
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
        Preconditions.checkNotNull(testConfig, "testConfig");
        Preconditions.checkNotNull(builderConfig, "builderConfig");

        this.testConfig = testConfig;
        this.state = new TestState();
        this.closed = new AtomicBoolean();
        this.actors = new ArrayList<>();
        this.executor = Executors.newScheduledThreadPool(
                testConfig.getThreadPoolSize(),
                new ThreadFactoryBuilder().setNameFormat("self-test-%d").build());
        this.store = createStoreAdapter(builderConfig);
        this.dataSource = new ProducerDataSource(this.testConfig, this.state, this.store);
        this.testCompletion = new AtomicReference<>();
        addListener(new ServiceShutdownListener(this::shutdownCallback, this::shutdownCallback), this.executor);
        this.reporter = new Reporter(this.state, this.testConfig, this.store::getStorePoolSnapshot, this.executor);
    }

    private StoreAdapter createStoreAdapter(ServiceBuilderConfig builderConfig) {
        if (this.testConfig.isUseClient()) {
            return new HostStoreAdapter(this.testConfig, builderConfig, this.executor);
        } else {
            return new StreamSegmentStoreAdapter(this.testConfig, builderConfig, this.executor);
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

            this.dataSource.deleteAllSegments()
                           .exceptionally(ex -> {
                               TestLogger.log(LOG_ID, "Unable to delete all segments: %s.", ex);
                               return null;
                           }).join();

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

        TestLogger.log(LOG_ID, "Starting.");

        // Create all segments, then start the Actor Manager.
        CompletableFuture<Void> startFuture = this.store
                .initialize(this.testConfig.getTimeout())
                .thenCompose(v -> this.dataSource.createSegments())
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
                            this.reporter.startAsync();
                        },
                        this.executor);

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
        // Create Producers (based on TestConfig).
        for (int i = 0; i < this.testConfig.getProducerCount(); i++) {
            this.actors.add(new Producer(Integer.toString(i), this.testConfig, this.dataSource, this.store, this.executor));
        }

        // Create Consumers (based on the number of non-transaction Segments).
        if (Consumer.canUseStoreAdapter(this.store)) {
            for (val si : this.state.getAllSegments()) {
                if (!si.isTransaction()) {
                    this.actors.add(new Consumer(si.getName(), this.testConfig, this.dataSource, this.state, this.store, this.executor));
                }
            }
        } else {
            TestLogger.log(LOG_ID, "Not creating any consumers because the StoreAdapter does not support all required features.");
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
