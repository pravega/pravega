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

import io.netty.util.internal.ConcurrentSet;
import io.pravega.client.ClientConfig;
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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.client.stream.mock.MockClientFactory;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScaleMonitor;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import io.pravega.test.integration.utils.IntegerSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Slf4j
public class EndToEndTransactionOrderTest {
    final StreamConfiguration config = StreamConfiguration.builder()
                                                          .scalingPolicy(ScalingPolicy.fixed(1))
                                                          .build();

    ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
    ConcurrentHashMap<String, List<UUID>> writersList = new ConcurrentHashMap<>();
    ConcurrentHashMap<Integer, UUID> eventToTxnMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<UUID, String> txnToWriter = new ConcurrentHashMap<>();
    AtomicInteger counter = new AtomicInteger();
    ConcurrentSet<UUID> uncommitted = new ConcurrentSet<>();
    TestingServer zkTestServer;
    ControllerWrapper controllerWrapper;
    Controller controller;
    ConnectionFactory connectionFactory;
    ClientFactoryImpl internalCF;
    PravegaConnectionListener server;
    AutoScaleMonitor autoScaleMonitor;
    MockClientFactory clientFactory;
    ReaderGroupManager readerGroupManager;
    EventStreamReader<Integer> reader;
    
    @Before
    public void setUp() throws Exception {

        zkTestServer = new TestingServerStarter().start();
        int port = Config.SERVICE_PORT;

        controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
        controller = controllerWrapper.getController();
        controllerWrapper.getControllerService().createScope(NameUtils.INTERNAL_SCOPE_NAME).get();

        connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());

        internalCF = new ClientFactoryImpl(NameUtils.INTERNAL_SCOPE_NAME, controller, connectionFactory);

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        autoScaleMonitor = new AutoScaleMonitor(store,
                internalCF,
                AutoScalerConfig.builder().with(AutoScalerConfig.MUTE_IN_SECONDS, 0)
                                .with(AutoScalerConfig.COOLDOWN_IN_SECONDS, 0).build());

        server = new PravegaConnectionListener(false, "localhost", 12345, store, tableStore,
                autoScaleMonitor.getStatsRecorder(), autoScaleMonitor.getTableSegmentStatsRecorder(), null, null, null, true);
        server.startListening();

        controllerWrapper.awaitRunning();
        controllerWrapper.getControllerService().createScope("test").get();

        controller.createStream("test", "test", config).get();

        clientFactory = new MockClientFactory("test", controller);
        readerGroupManager = new ReaderGroupManagerImpl("test", controller, clientFactory, connectionFactory);
        readerGroupManager.createReaderGroup("readergrp",
                                             ReaderGroupConfig.builder()
                                                              .automaticCheckpointIntervalMillis(2000)
                                                              .groupRefreshTimeMillis(1000)
                                                              .stream("test/test")
                                                              .build());

        reader = clientFactory.createReader("1",
                "readergrp",
                new IntegerSerializer(),
                ReaderConfig.builder().build());

    }

    @After
    public void tearDown() throws Exception {
        reader.close();
        readerGroupManager.close();
        controllerWrapper.close();
        autoScaleMonitor.close();
        clientFactory.close();
        internalCF.close();
        connectionFactory.close();
        server.close();
        zkTestServer.close();
    }
    
    @Test(timeout = 100000)
    public void testOrder() throws Exception {
        final AtomicBoolean done = new AtomicBoolean(false);

        CompletableFuture<Void> writer1 = startWriter("1", clientFactory, done);
        CompletableFuture<Void> writer2 = startWriter("2", clientFactory, done);
        CompletableFuture<Void> writer3 = startWriter("3", clientFactory, done);
        CompletableFuture<Void> writer4 = startWriter("4", clientFactory, done);

        // perform multiple scale stream operations so that rolling transactions may happen
        Stream s = new StreamImpl("test", "test");
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 1.0);
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        controller.scaleStream(s, Collections.singletonList(0L), map, executor).getFuture().get();

        controller.scaleStream(s, Collections.singletonList(StreamSegmentNameUtils.computeSegmentId(1, 1)), map, executor).getFuture().get();

        controller.scaleStream(s, Collections.singletonList(StreamSegmentNameUtils.computeSegmentId(2, 2)), map, executor).getFuture().get();

        // stop writers
        done.set(true);

        CompletableFuture.allOf(writer1, writer2, writer3, writer4).join();

        // wait for all transactions to commit
        Futures.allOf(eventToTxnMap.entrySet().stream()
                                   .map(x -> waitTillCommitted(controller, s, x.getValue(), uncommitted)).collect(Collectors.toList())).join();

        assertTrue(uncommitted.isEmpty());
        // read all events using a single reader and verify the order

        List<Triple<Integer, UUID, String>> eventOrder = new LinkedList<>();
        // create a reader
        while (!eventToTxnMap.isEmpty()) {
            EventRead<Integer> integerEventRead = reader.readNextEvent(SECONDS.toMillis(60));
            if (integerEventRead.getEvent() != null) {
                int event1 = integerEventRead.getEvent();
                UUID txnId = eventToTxnMap.remove(event1);
                String writerId = txnToWriter.get(txnId);
                UUID first = writersList.get(writerId).remove(0);
                eventOrder.add(new ImmutableTriple<>(event1, txnId, writerId));
                assertEquals(first, txnId);
            }
        }
    }

    private CompletableFuture<Void> waitTillCommitted(Controller controller, Stream s, UUID key, ConcurrentSet<UUID> uncommitted) {
        AtomicBoolean committed = new AtomicBoolean(false);
        AtomicInteger counter = new AtomicInteger(0);
        // check 6 times with 5 second gap until transaction is committed. if it is not committed, declare it uncommitted
        return Futures.loop(() -> !committed.get() && counter.getAndIncrement() < 5,
                () -> Futures.delayedFuture(() -> controller.checkTransactionStatus(s, key)
                                                            .thenAccept(status -> {
                                                                committed.set(status.equals(Transaction.Status.COMMITTED));
                                                            }), 5000, executor), executor)
                .thenAccept(v -> {
                    if (!committed.get()) {
                        uncommitted.add(key);
                    }
                });
    }

    private CompletableFuture<Void> startWriter(String writerId, MockClientFactory clientFactory, AtomicBoolean done) {
        EventWriterConfig writerConfig = EventWriterConfig.builder()
                                                          .transactionTimeoutTime(30000)
                                                          .build();
        TransactionalEventStreamWriter<Integer> test = clientFactory.createTransactionalEventWriter(writerId, "test", new IntegerSerializer(), writerConfig);
        List<UUID> list = new LinkedList<>();
        writersList.put(writerId, list);

        // Mocking pravega service by putting scale up and scale down requests for the stream
        return Futures.loop(() -> !done.get(),
                () -> Futures.delayedFuture(Duration.ofMillis(10), executor).thenAccept(v -> {
                    try {
                        Transaction<Integer> transaction = test.beginTxn();
                        transaction.getTxnId();
                        int i1 = counter.incrementAndGet();
                        transaction.writeEvent("0", i1);
                        transaction.commit();
                        list.add(transaction.getTxnId());
                        eventToTxnMap.put(i1, transaction.getTxnId());
                        txnToWriter.put(transaction.getTxnId(), writerId);
                    } catch (Throwable e) {
                        log.error("test exception writing events {}", e);
                        throw new CompletionException(e);
                    }
                }), executor)
                .thenAccept(v -> test.close());
    }
}
