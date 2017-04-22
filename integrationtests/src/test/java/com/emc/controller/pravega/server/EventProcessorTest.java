/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.controller.pravega.server;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.stream.Position;
import com.emc.pravega.testcommon.TestingServerStarter;
import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroup;
import com.emc.pravega.controller.eventProcessor.EventProcessorGroupConfig;
import com.emc.pravega.controller.eventProcessor.EventProcessorSystem;
import com.emc.pravega.controller.eventProcessor.ExceptionHandler;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorGroupConfigImpl;
import com.emc.pravega.controller.eventProcessor.impl.EventProcessorSystemImpl;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreFactory;
import com.emc.pravega.demo.ControllerWrapper;
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
import com.emc.pravega.stream.impl.ReaderGroupManagerImpl;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.testcommon.TestUtils;
import com.google.common.base.Preconditions;
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
        serviceBuilder.initialize().get();
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
                .scope(scope)
                .streamName(streamName)
                .scalingPolicy(ScalingPolicy.fixed(1))
                .build();

        System.err.println(String.format("Creating stream (%s, %s)", scope, streamName));
        CompletableFuture<Boolean> createStatus = controller.createStream(config);
        if (!createStatus.get()) {
            System.err.println("Stream alrady existed, exiting");
            return;
        }

        @Cleanup
        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(false);
        @Cleanup
        ClientFactory clientFactory = new ClientFactoryImpl(scope, controller, connectionFactory);

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
                new ReaderGroupManagerImpl(scope, controller, clientFactory));

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
