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
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.eventProcessor.EventProcessorConfig;
import io.pravega.controller.eventProcessor.EventProcessorGroup;
import io.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import io.pravega.controller.eventProcessor.EventProcessorSystem;
import io.pravega.controller.eventProcessor.ExceptionHandler;
import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import io.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import io.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.shared.controller.event.RequestProcessor;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.demo.ControllerWrapper;
import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

/**
 * End-to-end tests for event processor.
 */
@Slf4j
public class EventProcessorTest {

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
    }

    public static void main(String[] args) throws Exception {
        new EventProcessorTest().testEventProcessor();
        System.exit(0);
    }

    @Test(timeout = 60000)
    public void testEventProcessor() throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServerStarter().start();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int servicePort = TestUtils.getAvailableListenPort();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store);
        server.startListening();
        int controllerPort = TestUtils.getAvailableListenPort();
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(
                zkTestServer.getConnectString(),
                true,
                controllerPort,
                "localhost",
                servicePort,
                4);
        controllerWrapper.awaitRunning();
        Controller controller = controllerWrapper.getController();

        // Create controller object for testing against a separate controller process.
        // ControllerImpl controller = new ControllerImpl("localhost", 9090);

        final String host = "host";
        final String scope = "controllerScope";
        final String streamName = "stream1";
        final String readerGroup = "readerGroup";

        final CompletableFuture<Boolean> createScopeStatus = controller.createScope(scope);

        if (!createScopeStatus.join()) {
            throw new RuntimeException("Scope already existed");
        }

        final StreamConfiguration config = StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        System.err.println(String.format("Creating stream (%s, %s)", scope, streamName));
        CompletableFuture<Boolean> createStatus = controller.createStream(scope, streamName, config);
        if (!createStatus.get()) {
            System.err.println("Stream alrady existed, exiting");
            return;
        }

        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        @Cleanup
        ClientFactoryImpl clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

        @Cleanup
        EventStreamWriter<TestEvent> producer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(), EventWriterConfig.builder().build());

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
                .serializer(new JavaSerializer<>())
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
}
