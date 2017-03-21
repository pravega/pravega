/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse.ScaleStreamStatus;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.StreamImpl;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
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
@Slf4j
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
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString());
        Controller controller = controllerWrapper.getController();

        final String scope = "scope";
        controllerWrapper.getControllerService().createScope(scope).get();

        final String streamName = "stream1";
        final StreamConfiguration config =
                StreamConfiguration.builder().scope(scope).streamName(streamName).scalingPolicy(
                        ScalingPolicy.fixed(1)).build();

        Stream stream = new StreamImpl(scope, streamName);

        log.info("Creating stream {}/{}", scope, streamName);
        CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);
        if (createStatus.get().getStatus() != CreateStreamStatus.Status.SUCCESS) {
            log.error("Create stream failed, exiting");
            return;
        }

        // Test 1: scale stream: split one segment into two
        log.info("Scaling stream {}/{}, splitting one segment into two", scope, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);
        CompletableFuture<ScaleResponse> scaleResponseFuture =
                controller.scaleStream(stream, Collections.singletonList(0), map);
        ScaleResponse scaleResponse = scaleResponseFuture.get();
        if (scaleResponse.getStatus() != ScaleStreamStatus.SUCCESS) {
            log.error("Scale stream: splitting segment into two failed, exiting");
            return;
        }

        // Test 2: scale stream: merge two segments into one
        log.info("Scaling stream {}/{}, merging two segments into one", scope, streamName);
        scaleResponseFuture = controller.scaleStream(stream, Arrays.asList(1, 2), Collections.singletonMap(0.0, 1.0));
        scaleResponse = scaleResponseFuture.get();
        if (scaleResponse.getStatus() != ScaleStreamStatus.SUCCESS) {
            log.error("Scale stream: merging two segments into one failed, exiting");
            return;
        }

        // Test 3: create a transaction, and try scale operation, it should fail with precondition check failure
        CompletableFuture<UUID> txIdFuture = controller.createTransaction(stream, 5000, 3600000, 60000);
        UUID txId = txIdFuture.get();
        if (txId == null) {
            log.error("Create transaction failed, exiting");
            return;
        }

        log.info("Scaling stream {}/{}, splitting one segment into two, while transaction is ongoing",
                scope, streamName);
        scaleResponseFuture = controller.scaleStream(stream, Collections.singletonList(3), map);
        CompletableFuture<ScaleResponse> future = scaleResponseFuture.whenComplete((r, e) -> {
            if (e != null) {
                log.error("Failed: scale with ongoing transaction.", e);
            } else if (FutureHelpers.getAndHandleExceptions(
                    controller.checkTransactionStatus(stream, txId), RuntimeException::new) != Transaction.Status.OPEN) {
                log.info("Success: scale with ongoing transaction.");
            } else {
                log.error("Failed: scale with ongoing transaction.");
            }
        });

        CompletableFuture<Void> statusFuture = controller.abortTransaction(stream, txId);
        statusFuture.get();
        future.get();

        log.info("All scaling test PASSED");

        System.exit(0);
    }
}
