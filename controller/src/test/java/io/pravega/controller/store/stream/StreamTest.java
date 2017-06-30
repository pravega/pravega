/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.store.stream.tables.State;
import io.pravega.test.common.TestingServerStarter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;

public class StreamTest {
    private TestingServer zkTestServer;
    private CuratorFramework cli;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();
        cli = CuratorFrameworkFactory.newClient(zkTestServer.getConnectString(), new RetryOneTime(2000));
        cli.start();
    }

    @After
    public void tearDown() throws Exception {
        cli.close();
        zkTestServer.close();
        executor.shutdown();
    }

    @Test(timeout = 10000)
    public void testZkCreateStream() throws ExecutionException, InterruptedException {
        ZKStoreHelper zkStoreHelper = new ZKStoreHelper(cli, executor);
        ZKStream zkStream = new ZKStream("test", "test", zkStoreHelper);
        testStream(zkStream);
    }

    @Test(timeout = 10000)
    public void testInMemoryCreateStream() throws ExecutionException, InterruptedException {
        InMemoryStream stream = new InMemoryStream("test", "test");
        testStream(stream);
    }

    private void testStream(PersistentStreamBase<Integer> stream) throws InterruptedException, ExecutionException {
        long creationTime1 = System.currentTimeMillis();
        long creationTime2 = creationTime1 + 1;
        final ScalingPolicy policy1 = ScalingPolicy.fixed(5);
        final ScalingPolicy policy2 = ScalingPolicy.fixed(6);

        final StreamConfiguration streamConfig1 = StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(policy1).build();
        final StreamConfiguration streamConfig2 = StreamConfiguration.builder().scope("test").streamName("test").scalingPolicy(policy2).build();

        CreateStreamResponse response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        stream.storeCreationTimeIfAbsent(creationTime1).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());

        stream.createConfigurationIfAbsent(streamConfig1).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.createStateIfAbsent(State.UNKNOWN).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.updateState(State.CREATING).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.NEW, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_CREATING, response.getStatus());

        stream.updateState(State.ACTIVE).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());

        stream.updateState(State.SEALING).get();

        response = stream.checkStreamExists(streamConfig1, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime1).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
        response = stream.checkStreamExists(streamConfig2, creationTime2).get();
        assertEquals(CreateStreamResponse.CreateStatus.EXISTS_ACTIVE, response.getStatus());
    }

}
