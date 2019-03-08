/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.controller.server;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.ReaderGroupManagerImpl;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.EventSerializer;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * End-to-end tests for event processor.
 */
@Slf4j
public class EventProcessorTest {
    final String host = "host";

    TestingServer zkTestServer;
    PravegaConnectionListener server;
    ControllerWrapper controllerWrapper;
    Controller controller;
    EventSerializer<TestEvent> eventSerializer;
    
    public static class TestEventProcessor extends EventProcessor<TestEvent> {
        long sum;
        CompletableFuture<Long> result;
        final boolean throwErrors;

        public TestEventProcessor(Boolean throwErrors, CompletableFuture<Long> result) {
            Preconditions.checkNotNull(throwErrors);
            Preconditions.checkNotNull(result);
            sum = 0;
            this.result = result;
            this.throwErrors = throwErrors;
        }

        @Override
        protected void process(TestEvent event, Position position) {
            if (event.getNumber() < 0) {
                result.complete(sum);
                throw new RuntimeException();
            } else {
                int val = event.getNumber();
                sum += val;
                if (throwErrors && val % 2 == 0) {
                    throw new IllegalArgumentException();
                }
            }
        }
    }
    
    @Data
    @AllArgsConstructor
    @Builder
    public static class TestEvent implements ControllerEvent {
        private static final long serialVersionUID = 1L;
        int number;

        @Override
        public String getKey() {
            return null;
        }

        @Override
        public CompletableFuture<Void> process(RequestProcessor processor) {
            return Futures.failedFuture(new RuntimeException("This should not be called"));
        }

        static class TestEventBuilder implements ObjectBuilder<TestEvent> {

        }

        static class Serializer extends VersionedSerializer.WithBuilder<TestEvent, TestEventBuilder> {
            @Override
            protected TestEventBuilder newBuilder() {
                return TestEvent.builder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void write00(TestEvent e, RevisionDataOutput target) throws IOException {
                target.writeInt(e.number);
            }

            private void read00(RevisionDataInput source, TestEventBuilder b) throws IOException {
                b.number(source.readInt());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        new EventProcessorTest().testEventProcessor();
        System.exit(0);
    }

    @Before
    public void setUp() throws Exception {
        zkTestServer = new TestingServerStarter().start();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int servicePort = TestUtils.getAvailableListenPort();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore);
        server.startListening();
        int controllerPort = TestUtils.getAvailableListenPort();

        controllerWrapper = new ControllerWrapper(
                zkTestServer.getConnectString(),
                true,
                controllerPort,
                "localhost",
                servicePort,
                4);
        controllerWrapper.awaitRunning();
        controller = controllerWrapper.getController();
    }

    @After
    public void tearDown() throws Exception {
        controllerWrapper.close();
        server.close();
        zkTestServer.stop();   
    }
    
    @Test(timeout = 60000)
    public void testEventProcessor() throws Exception {
        final String scope = "controllerScope";
        final String streamName = "stream1";
        final String readerGroup = "readerGroup";

        controller.createScope(scope).join();

        final StreamConfiguration config = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                                              .build();

        controller.createStream(scope, streamName, config).join();

        eventSerializer = new EventSerializer<>(new TestSerializer());
    
        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        @Cleanup
        EventStreamWriter<TestEvent> producer = clientFactory.createEventWriter(streamName,
                eventSerializer, EventWriterConfig.builder().build());

        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int expectedSum = input.length * (input.length + 1) / 2;

        for (int i = 0; i < input.length; i++) {
            producer.writeEvent("key", new TestEvent(input[i]));
        }
        producer.writeEvent("key", new TestEvent(-1));
        producer.flush();

        EventProcessorSystem system = new EventProcessorSystemImpl("Controller", host, scope,
                new ClientFactoryImpl(scope, controller, connectionFactory),
                new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory));

        CheckpointConfig.CheckpointPeriod period =
                CheckpointConfig.CheckpointPeriod.builder()
                        .numEvents(1)
                        .numSeconds(1)
                        .build();

        CheckpointConfig checkpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.Periodic)
                        .checkpointPeriod(period)
                        .build();

        EventProcessorGroupConfig eventProcessorGroupConfig =
                EventProcessorGroupConfigImpl.builder()
                        .eventProcessorCount(1)
                        .readerGroupName(readerGroup)
                        .streamName(streamName)
                        .checkpointConfig(checkpointConfig)
                        .build();
        CompletableFuture<Long> result = new CompletableFuture<>();
        // Test case 1. Actor does not throw any exception during normal operation.
        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new TestEventProcessor(false, result))
                .serializer(eventSerializer)
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(eventProcessorGroupConfig)
                .build();
        @Cleanup
        EventProcessorGroup<TestEvent> eventProcessorGroup =
                system.createEventProcessorGroup(eventProcessorConfig, CheckpointStoreFactory.createInMemoryStore());

        Long value = result.join();
        Assert.assertEquals(expectedSum, value.longValue());
        log.info("SUCCESS: received expected sum = " + expectedSum);
    }
    
    @Test(timeout = 60000)
    public void testEventProcessorFailover() throws Exception {
        final String scope = "controllerScope2";
        final String streamName = "stream2";
        final String readerGroup = "readerGroup2";

        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());

        controller.createScope(scope).join();
        final StreamConfiguration config = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(1))
                                                              .build();
        controller.createStream(scope, streamName, config).join();
        eventSerializer = new EventSerializer<>(new TestSerializer());

        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        @Cleanup
        EventStreamWriter<TestEvent> producer = clientFactory.createEventWriter(streamName,
                eventSerializer, EventWriterConfig.builder().build());
        TestEvent event1 = new TestEvent(0);
        producer.writeEvent("key", event1).join();
        TestEvent event2 = new TestEvent(1);
        producer.writeEvent("key", event2).join();
        producer.flush();

        EventProcessorSystem system = new EventProcessorSystemImpl("Controller", host, scope,
                new ClientFactoryImpl(scope, controller, connectionFactory),
                new ReaderGroupManagerImpl(scope, controller, clientFactory, connectionFactory));
        
        CheckpointConfig checkpointConfig =
                CheckpointConfig.builder()
                        .type(CheckpointConfig.Type.None)
                        .build();

        EventProcessorGroupConfig eventProcessorGroupConfig =
                EventProcessorGroupConfigImpl.builder()
                        .eventProcessorCount(1)
                        .readerGroupName(readerGroup)
                        .streamName(streamName)
                        .checkpointConfig(checkpointConfig)
                        .build();

        LinkedBlockingQueue<TestEvent> eventsProcessed = new LinkedBlockingQueue<>();
        EventProcessorConfig<TestEvent> eventProcessorConfig = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new EventProcessor<TestEvent>() {
                    @Override
                    protected void process(TestEvent event, Position position) {
                        try {
                            eventsProcessed.offer(event);
                            // keep sending null position
                            getCheckpointer().store(null);
                        } catch (CheckpointStoreException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .serializer(eventSerializer)
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(eventProcessorGroupConfig)
                .build();
        @Cleanup
        EventProcessorGroup<TestEvent> eventProcessorGroup =
                system.createEventProcessorGroup(eventProcessorConfig, CheckpointStoreFactory.createInMemoryStore());

        eventProcessorGroup.awaitRunning();
        // wait until both events are read
        assertEquals(event1, eventsProcessed.take());
        assertEquals(event2, eventsProcessed.take());
        
        assertTrue(eventsProcessed.isEmpty());
        
        // shutdown event processor
        // upon shutdown readerGroup.offline and reader.close should have been called. 
        eventProcessorGroup.stopAsync();
        eventProcessorGroup.awaitTerminated();

        @Cleanup
        ConnectionFactoryImpl connectionFactory2 = new ConnectionFactoryImpl(ClientConfig.builder().build());

        @Cleanup
        ClientFactoryImpl clientFactory2 = new ClientFactoryImpl(scope, controller, connectionFactory2);

        system = new EventProcessorSystemImpl("Controller2", host, scope,
                new ClientFactoryImpl(scope, controller, connectionFactory2),
                new ReaderGroupManagerImpl(scope, controller, clientFactory2, connectionFactory2));

        EventProcessorConfig<TestEvent> eventProcessorConfig2 = EventProcessorConfig.<TestEvent>builder()
                .supplier(() -> new EventProcessor<TestEvent>() {
                    @Override
                    protected void process(TestEvent event, Position position) {
                        try {
                            eventsProcessed.offer(event);
                            getCheckpointer().store(null);
                        } catch (CheckpointStoreException e) {
                            e.printStackTrace();
                        }
                    }
                })
                .serializer(eventSerializer)
                .decider((Throwable e) -> ExceptionHandler.Directive.Stop)
                .config(eventProcessorGroupConfig)
                .build();

        @Cleanup
        EventProcessorGroup<TestEvent> eventProcessorGroup2 =
                system.createEventProcessorGroup(eventProcessorConfig2, CheckpointStoreFactory.createInMemoryStore());
        eventProcessorGroup2.awaitRunning();

        // verify that both events are read again
        assertEquals(event1, eventsProcessed.take());
        assertEquals(event2, eventsProcessed.take());
        assertTrue(eventsProcessed.isEmpty());
        
        eventProcessorGroup2.stopAsync();
        eventProcessorGroup2.awaitTerminated();
    }

    private static class TestSerializer extends ControllerEventSerializer {
        @Override
        protected void declareSerializers(Builder builder) {
            super.declareSerializers(builder);
            builder.serializer(TestEvent.class, 127, new TestEvent.Serializer());
        }
    }
}
