/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.index.HostIndex;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.State;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import io.pravega.shared.controller.event.ScaleOpEvent;
import io.pravega.test.common.TestingServerStarter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

/**
 * RequestSweeper test cases.
 */
@Slf4j
public abstract class RequestSweeperTest {
    private static final String HOSTNAME = "host-1234";
    private static final String SCOPE = "scope";
    protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    protected CuratorFramework cli;

    private final String stream1 = "stream1";

    private StreamMetadataStore streamStore;

    private final HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());

    private TestingServer zkServer;

    private StreamMetadataTasks streamMetadataTasks;
    private EventStreamWriterMock<ControllerEvent> requestEventWriter;
    private SegmentHelper segmentHelperMock;
    private final RequestTracker requestTracker = new RequestTracker(true);

    abstract StreamMetadataStore getStream();

    @Before
    public void setUp() throws Exception {
        zkServer = new TestingServerStarter().start();
        zkServer.start();

        cli = CuratorFrameworkFactory.newClient(zkServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
        streamStore = getStream();

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder()
                                                                                        .controllerURI(URI.create("tcp://localhost"))
                                                                                        .build());
        segmentHelperMock = SegmentHelperMock.getSegmentHelperMock();
        streamMetadataTasks = new StreamMetadataTasks(streamStore, StreamStoreFactory.createInMemoryBucketStore(),
                TaskStoreFactory.createInMemoryStore(executor), segmentHelperMock, executor, HOSTNAME, AuthHelper.getDisabledAuthHelper(), 
                requestTracker);
        requestEventWriter = spy(new EventStreamWriterMock<>());
        streamMetadataTasks.setRequestEventWriter(requestEventWriter);

        final ScalingPolicy policy1 = ScalingPolicy.fixed(2);
        final StreamConfiguration configuration1 = StreamConfiguration.builder().scalingPolicy(policy1).build();

        // region createStream
        streamStore.createScope(SCOPE).join();
        long start = System.currentTimeMillis();
        streamStore.createStream(SCOPE, stream1, configuration1, start, null, executor).join();
        streamStore.setState(SCOPE, stream1, State.ACTIVE, null, executor).join();
        // endregion
    }

    @After
    public void tearDown() throws Exception {
        streamMetadataTasks.close();
        streamStore.close();
        cli.close();
        zkServer.stop();
        zkServer.close();
        ExecutorServiceHelpers.shutdown(executor);
    }

    @Test(timeout = 30000)
    public void testRequestSweeper() throws ExecutionException, InterruptedException {
        AbstractMap.SimpleEntry<Double, Double> segment1 = new AbstractMap.SimpleEntry<>(0.5, 0.75);
        AbstractMap.SimpleEntry<Double, Double> segment2 = new AbstractMap.SimpleEntry<>(0.75, 1.0);
        List<Long> sealedSegments = Collections.singletonList(1L);

        CompletableFuture<Void> wait1 = new CompletableFuture<>();
        CompletableFuture<Void> wait2 = new CompletableFuture<>();
        LinkedBlockingQueue<CompletableFuture<Void>> waitQueue = new LinkedBlockingQueue<>();
        waitQueue.put(wait1);
        waitQueue.put(wait2);
        CompletableFuture<Void> signal1 = new CompletableFuture<>();
        CompletableFuture<Void> signal2 = new CompletableFuture<>();
        LinkedBlockingQueue<CompletableFuture<Void>> signalQueue = new LinkedBlockingQueue<>();
        signalQueue.put(signal1);
        signalQueue.put(signal2);
        doAnswer(x -> {
            log.info("shivesh:: write event called, completing future");
            signalQueue.take().complete(x.getArgument(0));
            CompletableFuture<Void> taken = waitQueue.take();
            log.info("shivesh:: completed future.. taken from wait queue");
            return taken;
        }).when(requestEventWriter).writeEvent(anyString(), any());
        
        streamMetadataTasks.manualScale(SCOPE, stream1, sealedSegments, Arrays.asList(segment1, segment2), 
                System.currentTimeMillis(), null);

        log.info("shivesh:: submitted manual scale.. got a future back.. now waiting on the signal future that will be completed in writeEvent");

        signal1.join();

        log.info("shivesh:: manual scale request future is completed");
        // since we dont complete writeEventFuture, manual scale will not complete and index is not removed
        // verify that index has the entry.
        HostIndex hostIndex = getHostIndex();
        List<String> entities = hostIndex.getEntities(HOSTNAME).join();
        assertEquals(1, entities.size());
        byte[] data = hostIndex.getEntityData(HOSTNAME, entities.get(0)).join();
        ControllerEventSerializer serializer = new ControllerEventSerializer();
        ControllerEvent event = serializer.fromByteBuffer(ByteBuffer.wrap(data));
        assertTrue(event instanceof ScaleOpEvent);
        
        RequestSweeper requestSweeper = new RequestSweeper(streamStore, executor, streamMetadataTasks);
        CompletableFuture<Void> failoverFuture = requestSweeper.handleFailedProcess(HOSTNAME);

        // verify that the event is posted.. signal 2 future should be completed. 
        signal2.join();
        // let wait2 be complete as well. 
        wait2.complete(null);
        
        // wait for failover to complete
        failoverFuture.join();
        
        // verify that entity is removed. 
        entities = hostIndex.getEntities(HOSTNAME).join();
        assertTrue(entities.isEmpty());
    }

    abstract HostIndex getHostIndex();
}