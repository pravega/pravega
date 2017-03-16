/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.store.client.StoreClientFactory;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;

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
        protected void process(TestEvent event) {
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
    public static class TestEvent implements ControllerEvent, Serializable {
        int number;
    }

    public static void main(String[] args) throws Exception {
        new EventProcessorTest().testEventProcessor();
    }

    @Test
    public void testEventProcessor() throws Exception {
        TestingServer zkTestServer = new TestingServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString());
        Controller controller = controllerWrapper.getController();

        // Create controller object for testing against a separate controller process.
        // ControllerImpl controller = new ControllerImpl("localhost", 9090);

        final String host = "host";
        final String scope = "controllerScope";
        final String streamName = "stream1";
        final String readerGroup = "readerGroup";

        final CompletableFuture<CreateScopeStatus> createScopeStatus = controller.createScope(scope);
        final CreateScopeStatus scopeStatus = createScopeStatus.join();

        if (CreateScopeStatus.Status.SUCCESS != scopeStatus.getStatus()) {
            throw new RuntimeException("Error creating scope");
        }

        final StreamConfiguration config = StreamConfiguration.builder()
                .scope(scope)
                .streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        System.err.println(String.format("Creating stream (%s, %s)", scope, streamName));
        CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);
        if (createStatus.get().getStatus() != CreateStreamStatus.Status.SUCCESS) {
            System.err.println("Create stream failed, exiting");
            return;
        }

        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller);

        EventStreamWriter<TestEvent> producer = clientFactory.createEventWriter(streamName,
                new JavaSerializer<>(), EventWriterConfig.builder().build());

        int[] input = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        int expectedSum = input.length * (input.length + 1) / 2;

        for (int i = 0; i < input.length; i++) {
            producer.writeEvent("key", new TestEvent(input[i]));
        }
        producer.writeEvent("key", new TestEvent(-1));
        producer.flush();

        EventProcessorSystem system = new EventProcessorSystemImpl("Controller", host, scope, controller);

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
        EventProcessorGroup<TestEvent> eventEventProcessorGroup =
                system.createEventProcessorGroup(eventProcessorConfig, StoreClientFactory.createInMemoryStoreClient());

        Long value = result.join();
        Assert.assertEquals(expectedSum, value.longValue());
        log.info("SUCCESS: received expected sum = " + expectedSum);

        producer.close();
        eventEventProcessorGroup.stopAll();
        server.close();
        zkTestServer.close();

        System.exit(0);
    }
}
