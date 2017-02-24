/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.ScaleStreamStatus;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * End to end scale tests.
 */
public class ScaleTest {
    @SuppressWarnings("checkstyle:ReturnCount")
    public static void main(String[] args) throws Exception {
        TestingServer zkTestServer = new TestingServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        // Create controller object for testing against a separate controller report.
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), true);
        Controller controller = controllerWrapper.getController();

        final String scope = "scope";
        controllerWrapper.getControllerService().createScope(scope).get();

        final String streamName = "stream1";
        final StreamConfiguration config =
                StreamConfiguration.builder().scope(scope).streamName(streamName).scalingPolicy(
                        ScalingPolicy.fixed(1)).build();

        Stream stream = new StreamImpl(scope, streamName);

        System.err.println(String.format("Creating stream (%s, %s)", scope, streamName));
        CreateStreamStatus createStatus = controller.createStream(config).get();
        if (createStatus != CreateStreamStatus.SUCCESS) {
            System.err.println("Create stream failed, exiting");
            return;
        }

        // Test 1: scale stream: split one segment into two
        System.err.println(String.format("Scaling stream (%s, %s), splitting one segment into two", scope, streamName));
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        CompletableFuture<ScaleResponse> scaleResponseFuture =
                controller.scaleStream(stream, Collections.singletonList(0), map);
        ScaleResponse scaleResponse = scaleResponseFuture.get();
        if (scaleResponse.getStatus() != ScaleStreamStatus.SUCCESS) {
            System.err.println("Scale stream: splitting segment into two failed, exiting");
            return;
        }

        // Test 2: scale stream: merge two segments into one
        System.err.println(String.format("Scaling stream (%s, %s), merging two segments into one", scope, streamName));
        scaleResponseFuture = controller.scaleStream(stream, Arrays.asList(1, 2), Collections.singletonMap(0.0, 1.0));
        scaleResponse = scaleResponseFuture.get();
        if (scaleResponse.getStatus() != ScaleStreamStatus.SUCCESS) {
            System.err.println("Scale stream: merging two segments into one failed, exiting");
            return;
        }

        // Test 3: create a transaction, and try scale operation, it should fail with precondition check failure
        CompletableFuture<UUID> txIdFuture = controller.createTransaction(stream, 60000);
        UUID txId = txIdFuture.get();
        if (txId == null) {
            System.err.println("Create transaction failed, exiting");
            return;
        }

        System.err.println(
                String.format(
                        "Scaling stream (%s, %s), splitting one segment into two, while transaction is ongoing",
                        scope,
                        streamName));
        scaleResponseFuture = controller.scaleStream(stream, Collections.singletonList(3), map);
        scaleResponse = scaleResponseFuture.get();
        if (scaleResponse.getStatus() != ScaleStreamStatus.TXN_CONFLICT) {
            System.err.println("Scale stream while transaction is ongoing failed, exiting");
            return;
        }

        CompletableFuture<Void> statusFuture = controller.abortTransaction(stream, txId);
        statusFuture.get();

        // Test 4: try scale operation after transaction is dropped
        System.err.println(
                String.format(
                        "Scaling stream (%s, %s), splitting one segment into two, after transaction is dropped",
                        scope,
                        streamName));

        scaleResponseFuture = controller.scaleStream(stream, Collections.singletonList(3), map);
        scaleResponse = scaleResponseFuture.get();
        if (scaleResponse.getStatus() != ScaleStreamStatus.SUCCESS) {
            System.err.println("Scale stream after transaction is dropped failed, exiting");
            return;
        }

        System.err.println("All scaling test PASSED");

        System.exit(0);
    }
}
