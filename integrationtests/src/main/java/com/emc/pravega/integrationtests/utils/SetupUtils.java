/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.integrationtests.utils;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.StreamManager;
import com.emc.pravega.connectors.IntegerSerializer;
import com.emc.pravega.controller.util.Config;
import com.emc.pravega.demo.ControllerWrapper;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroupConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.testcommon.TestUtils;
import com.google.common.base.Preconditions;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Utility functions for creating the test setup.
 */
@Slf4j
@NotThreadSafe
public final class SetupUtils {
    // The controller endpoint.
    @Getter
    private URI controllerUri = null;

    // The different services.
    private ControllerWrapper controllerWrapper = null;
    private PravegaConnectionListener server = null;
    private TestingServer zkTestServer = null;

    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);

    // The test Scope name.
    @Getter
    private final String scope = "scope";

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public void startAllServices() throws Exception {
        if (!this.started.compareAndSet(false, true)) {
            log.warn("Services already started, not attempting to start again");
            return;
        }

        // Start zookeeper.
        this.zkTestServer = new TestingServer();
        this.zkTestServer.start();

        // Start Pravega Service.
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int servicePort = TestUtils.randomPort();
        this.server = new PravegaConnectionListener(false, servicePort, store);
        this.server.startListening();
        log.info("Started Pravega Service");

        // Start Controller.
        int controllerPort = TestUtils.randomPort();
        this.controllerWrapper = new ControllerWrapper(
                this.zkTestServer.getConnectString(), true, true, controllerPort, "localhost", servicePort,
                Config.HOST_STORE_CONTAINER_COUNT);
        this.controllerWrapper.awaitRunning();
        this.controllerWrapper.getController().createScope(this.scope).get();
        this.controllerUri = URI.create("tcp://localhost:" + String.valueOf(controllerPort));
        log.info("Initialized Pravega Controller");
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        this.controllerWrapper.close();
        this.server.close();
        this.zkTestServer.close();
    }

    /**
     * Create the test stream.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.create(this.controllerUri);
        streamManager.createScope(this.scope);
        streamManager.createStream(this.scope, streamName,
                StreamConfiguration.builder()
                        .scope(this.scope)
                        .streamName(streamName)
                        .scalingPolicy(ScalingPolicy.fixed(numSegments))
                        .build());
        log.info("Created stream: " + streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, this.controllerUri);
        return clientFactory.createEventWriter(
                streamName,
                new IntegerSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream reader instance.
     */
    public EventStreamReader<Integer> getIntegerReader(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(this.scope, this.controllerUri);
        final String readerGroup = "testReaderGroup" + this.scope + streamName;
        readerGroupManager.createReaderGroup(
                readerGroup,
                ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(streamName));

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, this.controllerUri);
        final String readerGroupId = UUID.randomUUID().toString();
        return clientFactory.createReader(
                readerGroupId,
                readerGroup,
                new IntegerSerializer(),
                ReaderConfig.builder().build());
    }
}
