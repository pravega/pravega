/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.client.ClientConfig;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.impl.UTF8StringSerializer;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import static io.pravega.test.common.AssertExtensions.assertEventuallyEquals;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.URI;
import static io.pravega.local.LocalPravegaEmulator.LocalPravegaEmulatorBuilder;

@Slf4j
public class EndToEndTxnWithTest extends ThreadPooledTestSuite {

    private static final String STREAM = "stream";
    private static final String SCOPE = "scope";
    LocalPravegaEmulator localPravega;
    private final int controllerPort = TestUtils.getAvailableListenPort();
    private final String serviceHost = "localhost";
    private final int servicePort = TestUtils.getAvailableListenPort();
    private final int containerCount = 4;
    private TestingServer zkTestServer;
    private PravegaConnectionListener server;
    private ServiceBuilder serviceBuilder;
    private  URI controllerUri = null;
    private StreamManager streamManager = null;


   @Override
   protected int getThreadPoolSize() {
       return 1;
   }

    @Before
    public void setUp() throws Exception {
        boolean authEnabled = false;
        boolean tlsEnabled = false;
        boolean restEnabled = true;
        log.info("standaloneRunning: {}", standaloneRunning());
        if (!standaloneRunning()) {
            ServiceBuilderConfig config = ServiceBuilderConfig
                .builder()
                .include(System.getProperties())
                .build();
            SingleNodeConfig conf = config.getConfig(SingleNodeConfig::builder);

            LocalPravegaEmulatorBuilder emulatorBuilder = LocalPravegaEmulator.builder()
                .controllerPort(TestUtils.getAvailableListenPort())
                .segmentStorePort(TestUtils.getAvailableListenPort())
                .zkPort(TestUtils.getAvailableListenPort())
                .restServerPort(TestUtils.getAvailableListenPort())
                .enableRestServer(restEnabled)
                .enableAuth(authEnabled)
                .enableTls(tlsEnabled);

            localPravega = emulatorBuilder.build();
            localPravega.start();
            controllerUri = URI.create(localPravega.getInProcPravegaCluster().getControllerURI());
        } else {
            controllerUri = URI.create("tcp://localhost:9090");
        }
    }

    @After
    public void tearDown() throws Exception {
        if (localPravega != null) {
            localPravega.close();
        }
    
    }

    boolean standaloneRunning() {
        try {
            ServerSocket serverSocket = new ServerSocket(9090);
            serverSocket.close();
            } catch (IOException e) {
                 log.info("port is in use");
                 return true;
            }
        return false;
    }

    @Test(timeout = 10000)
    public void testTxnWithScale() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();

        StreamManager streamManager = StreamManager.create(clientConfig);
        assertNotNull(streamManager);

        assertTrue("Creating scope", streamManager.createScope("test")); 
        assertTrue("Creating stream", streamManager.createStream("test", "test", config));

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                                                                            connectionFactory.getInternalExecutor());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl("test", controller, connectionFactory);
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter("test", new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction1 = test.beginTxn();
        transaction1.writeEvent("0", "txntest1");
        transaction1.commit();

        assertEventuallyEquals(Transaction.Status.COMMITTED, () -> transaction1.checkStatus(), 5000);
        
        // scale
        Stream stream = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        Boolean result = controller.scaleStream(stream, Collections.singletonList(0L), map, executorService()).getFuture().get();
        assertTrue(result);

        Transaction<String> transaction2 = test.beginTxn();
        transaction2.writeEvent("0", "txntest2");
        transaction2.commit();
        @Cleanup
        ReaderGroupManager groupManager = new ReaderGroupManagerImpl("test", controller, clientFactory, connectionFactory);
        groupManager.createReaderGroup("reader", ReaderGroupConfig.builder().disableAutomaticCheckpoints().groupRefreshTimeMillis(0).stream("test/test").build());
        @Cleanup
        EventStreamReader<String> reader = clientFactory.createReader("readerId", "reader", new UTF8StringSerializer(),
                ReaderConfig.builder().build());
        EventRead<String> event = reader.readNextEvent(5000);
        assertNotNull(event.getEvent());
        assertEquals("txntest1", event.getEvent());
        assertNull(reader.readNextEvent(100).getEvent());
        groupManager.getReaderGroup("reader").initiateCheckpoint("cp", executorService());
        event = reader.readNextEvent(5000);
        assertEquals("cp", event.getCheckpointName());
        event = reader.readNextEvent(5000);
        assertNotNull(event.getEvent());
        assertEquals("txntest2", event.getEvent());
    }

    @Test(timeout = 30000)
    public void testTxnWithErrors() throws Exception {
        StreamConfiguration config = StreamConfiguration.builder()
                                                        .scalingPolicy(ScalingPolicy.fixed(1))
                                                        .build();
        ClientConfig clientConfig = ClientConfig.builder().controllerURI(controllerUri).build();
        streamManager = StreamManager.create(ClientConfig.builder().controllerURI(controllerUri).build());

        assertTrue("Creating scope", streamManager.createScope(SCOPE));
        log.info("scope is created");
        assertTrue("Creating stream", streamManager.createStream(SCOPE, STREAM, config));
        log.info("stream is created");

        @Cleanup
        ConnectionFactory connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(clientConfig).build(),
                                                                            connectionFactory.getInternalExecutor());

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(SCOPE, controller, connectionFactory);
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter(STREAM, new UTF8StringSerializer(),
                EventWriterConfig.builder().transactionTimeoutTime(10000).build());
        Transaction<String> transaction = test.beginTxn();
        transaction.writeEvent("0", "txntest1");
        //abort the transaction to simulate a txn abort due to a missing ping request.
        controller.abortTransaction(Stream.of(SCOPE, STREAM), transaction.getTxnId()).join();
        //check the status of the transaction.
        assertEventuallyEquals(Transaction.Status.ABORTED, () -> controller.checkTransactionStatus(Stream.of(SCOPE, STREAM), transaction.getTxnId()).join(), 10000);
        transaction.writeEvent("0", "txntest2");
        //verify that commit fails with TxnFailedException.
        assertThrows("TxnFailedException should be thrown", () -> transaction.commit(), t -> t instanceof TxnFailedException);
    }


    private UUID createTxn(EventStreamClientFactory clientFactory, EventWriterConfig config, String streamName) {
        @Cleanup
        TransactionalEventStreamWriter<String> test = clientFactory.createTransactionalEventWriter(streamName, new JavaSerializer<>(),
                config);
        return test.beginTxn().getTxnId();
    }
}
