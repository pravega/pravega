/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.endtoendtest;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.common.ThreadPooledTestSuite;
import io.pravega.test.integration.demo.ControllerWrapper;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTxnWithTest extends ThreadPooledTestSuite {

    private static final String STREAM = "stream";
    private static final String SCOPE = "scope";

    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ControllerWrapper controllerWrapper;
    private ServiceBuilder serviceBuilder;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();

        server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(),
                false,
                controllerPort,
                serviceHost,
                servicePort,
                containerCount);
        controllerWrapper.awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        serviceBuilder.close();
        zkTestServer.close();
    }

    @Test(timeout = 10000)
    public void testTxnWithScale() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> test = clientFactory.createEventWriter("test", new JavaSerializer<>(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction = test.beginTxn();
        transaction.writeEvent("0", "txntest1");
        transaction.commit();

        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();

        assertTrue(result);

        transaction = test.beginTxn();
        transaction.writeEvent("0", "txntest2");
        transaction.commit();
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory, connectionFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().stream("test/test").build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new JavaSerializer<>(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("txntest1", event.getEvent());
        event = reader.readNextEvent(10000);
        assertNotNull(event);
        assertEquals("txntest2", event.getEvent());
    }

    @Test(timeout = 30000)
    public void testTxnWithErrors() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                                                        .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(SCOPE).get();
        controller.createStream(SCOPE, STREAM, config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        EventStreamWriter<String> test = clientFactory.createEventWriter(STREAM, new JavaSerializer<>(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction = test.beginTxn();
        transaction.writeEvent("0", "txntest1");
        //abort the transaction to simulate a txn abort due to a missing ping request.
        controller.abortTransaction(Stream.of(SCOPE, STREAM), transaction.getTxnId()).join();
        TimeUnit.SECONDS.sleep(10);
        //check the status of the transaction.
        Transaction.Status status = controller.checkTransactionStatus(Stream.of(SCOPE, STREAM), transaction.getTxnId()).join();
        assertEquals("Transaction status should be Aborted", Transaction.Status.ABORTED, status);
        transaction.writeEvent("0", "txntest2");
        //verify that commit fails with TxnFailedException.
        AssertExtensions.assertThrows("TxnFailedException should be thrown", () -> transaction.commit(), t -> t instanceof TxnFailedException);
    }



    @Test(timeout = 10000)
    public void testTxnConfig() throws Exception {
        // create stream test
        StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(10, 2, 1))
                .build();
        Controller controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope("test").get();
        controller.createStream("test", "test", config).get();
        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);

        // create writers with different configs and try creating transactions against those configs
        EventWriterConfig defaultConfig = EventWriterConfig.builder().build();
        assertNotNull(createTxn(clientFactory, defaultConfig, "test"));

        EventWriterConfig validConfig = EventWriterConfig.builder().transactionTimeoutTime(10000).build();
        assertNotNull(createTxn(clientFactory, validConfig, "test"));

        EventWriterConfig lowTimeoutConfig = EventWriterConfig.builder().transactionTimeoutTime(1000).build();
        AssertExtensions.assertThrows("low timeout period not honoured",
                () -> createTxn(clientFactory, lowTimeoutConfig, "test"), e -> e.getCause() instanceof IllegalArgumentException);

        EventWriterConfig highTimeoutConfig = EventWriterConfig.builder().transactionTimeoutTime(200 * 1000).build();
        AssertExtensions.assertThrows("high timeouot period not honoured",
                () -> createTxn(clientFactory, highTimeoutConfig, "test"), e -> e.getCause() instanceof IllegalArgumentException);
    }

    private UUID createTxn(ClientFactory clientFactory, EventWriterConfig config, String streamName) {
        @Cleanup
        EventStreamWriter<String> test = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                config);
        return test.beginTxn().getTxnId();
    }
}
