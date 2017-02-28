/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.connectors;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.ReaderGroupManager;
import com.emc.pravega.StreamManager;
import com.emc.pravega.controller.server.rpc.RPCServer;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.server.rpc.v1.ControllerServiceAsyncImpl;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamStoreFactory;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.task.TaskSweeper;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
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
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;

/**
 * Utility functions for creating the test setup.
 */
@Slf4j
public final class SetupUtils {
    // The controller endpoint.
    public static final URI CONTROLLER_URI = URI.create("tcp://localhost:9090");

    // The pravega service listening port.
    private static final int SERVICE_PORT = 12345;

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public static void startPravegaServices() throws Exception {
        startLocalService();
        startLocalController();
    }

    /**
     * Create the test stream.
     *
     * @param scope          Scope for the test stream.
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public static void createTestStream(final String scope, final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.withScope(scope, CONTROLLER_URI);
        streamManager.createStream(streamName,
                                   StreamConfiguration.builder()
                                                      .scope(scope)
                                                      .streamName(streamName)
                                                      .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                      .build());
        log.info("Created stream: " + streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param scope         Scope for the test stream.
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public static EventStreamWriter<Integer> getIntegerWriter(final String scope, final String streamName) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);

        ClientFactory clientFactory = ClientFactory.withScope(scope, SetupUtils.CONTROLLER_URI);
        return clientFactory.createEventWriter(
                streamName,
                new Serializer<Integer>() {
                    @Override
                    public ByteBuffer serialize(Integer value) {
                        return ByteBuffer.wrap(String.valueOf(value).getBytes());
                    }

                    @Override
                    public Integer deserialize(ByteBuffer serializedValue) {
                        return null;
                    }
                },
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param scope         Scope for the test stream.
     * @param streamName    Name of the test stream.
     *
     * @return Stream reader instance.
     */
    public static EventStreamReader<Integer> getIntegerReader(final String scope, final String streamName) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(streamName);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, SetupUtils.CONTROLLER_URI);
        final String readerGroup = "testReaderGroup" + scope + streamName;
        readerGroupManager.createReaderGroup(
                readerGroup,
                ReaderGroupConfig.builder().startingTime(0).build(),
                Collections.singleton(streamName));

        ClientFactory clientFactory = ClientFactory.withScope(scope, SetupUtils.CONTROLLER_URI);
        final String readerGroupId = UUID.randomUUID().toString();
        return clientFactory.createReader(
                readerGroupId,
                readerGroup,
                new Serializer<Integer>() {
                    @Override
                    public ByteBuffer serialize(Integer value) {
                        return null;
                    }

                    @Override
                    public Integer deserialize(ByteBuffer serializedValue) {

                        return Integer.valueOf(new String(serializedValue.array()));
                    }
                },
                ReaderConfig.builder().build());
    }

    // Start pravega service on localhost.
    private static void startLocalService() throws Exception {
        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        PravegaConnectionListener server = new PravegaConnectionListener(false, SERVICE_PORT, store);
        server.startListening();

        log.info("Started Pravega Service");
    }

    // Start controller on localhost.
    private static void startLocalController() throws Exception {
        TestingServer zkServer = new TestingServer();
        zkServer.start();

        String hostId;
        try {
            //On each controller process restart, it gets a fresh hostId,
            //which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            log.debug("Failed to get host address.", e);
            hostId = UUID.randomUUID().toString();
        }

        CuratorFramework zkClient = CuratorFrameworkFactory.newClient(zkServer.getConnectString(),
                                                                      new ExponentialBackoffRetry(200, 10, 5000));
        zkClient.start();

        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(
                20,
                new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());

        StoreClient storeClient = new ZKStoreClient(zkClient);

        final StreamMetadataStore streamStore = StreamStoreFactory.createStore(
                StreamStoreFactory.StoreType.InMemory,
                executor);

        final TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        final HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(
                streamStore, hostStore, taskMetadataStore, executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks =
                new StreamTransactionMetadataTasks(streamStore, hostStore, taskMetadataStore, executor, hostId);

        ControllerService controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks,
                                                                    streamTransactionMetadataTasks);
        RPCServer.start(new ControllerServiceAsyncImpl(controllerService));

        TaskSweeper taskSweeper = new TaskSweeper(taskMetadataStore, hostId, streamMetadataTasks,
                                                  streamTransactionMetadataTasks);
        log.info("Started Pravega Controller");
    }
}
