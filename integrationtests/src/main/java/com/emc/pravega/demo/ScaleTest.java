/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.util.Config;
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
import com.emc.pravega.stream.impl.TxnSegments;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import static com.emc.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

/**
 * End to end scale tests.
 */
@Slf4j
public class ScaleTest {
    
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int port = Config.SERVICE_PORT;
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        // Create controller object for testing against a separate controller report.
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString(), port);
        Controller controller = controllerWrapper.getController();

        final String scope = "scope";
        controllerWrapper.getControllerService().createScope(scope).get();

        final String streamName = "stream1";
        final StreamConfiguration config =
                StreamConfiguration.builder().scope(scope).streamName(streamName).scalingPolicy(
                        ScalingPolicy.fixed(1)).build();

        Stream stream = new StreamImpl(scope, streamName);

        log.info("Creating stream {}/{}", scope, streamName);
        if (!controller.createStream(config).get()) {
            log.error("Stream already existed, exiting");
            return;
        }

        // Test 1: scale stream: split one segment into two
        log.info("Scaling stream {}/{}, splitting one segment into two", scope, streamName);
        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);

        if (!controller.scaleStream(stream, Collections.singletonList(0), map).get()) {
            log.error("Scale stream: splitting segment into two failed, exiting");
            return;
        }

        // Test 2: scale stream: merge two segments into one
        log.info("Scaling stream {}/{}, merging two segments into one", scope, streamName);
        CompletableFuture<Boolean> scaleResponseFuture = controller.scaleStream(stream, Arrays.asList(1, 2), Collections.singletonMap(0.0, 1.0));
        
        if (!scaleResponseFuture.get()) {
            log.error("Scale stream: merging two segments into one failed, exiting");
            return;
        }

        // Test 3: create a transaction, and try scale operation, it should fail with precondition check failure
        CompletableFuture<TxnSegments> txnFuture = controller.createTransaction(stream, 5000, 3600000, 29000);
        TxnSegments transaction = txnFuture.get();
        if (transaction == null) {
            log.error("Create transaction failed, exiting");
            return;
        }

        log.info("Scaling stream {}/{}, splitting one segment into two, while transaction is ongoing",
                scope, streamName);
        scaleResponseFuture = controller.scaleStream(stream, Collections.singletonList(3), map);
        CompletableFuture<Boolean> future = scaleResponseFuture.whenComplete((r, e) -> {
            if (e != null) {
                log.error("Failed: scale with ongoing transaction.", e);
            } else if (getAndHandleExceptions(controller.checkTransactionStatus(stream, transaction.getTxnId()),
                    RuntimeException::new) != Transaction.Status.OPEN) {
                log.info("Success: scale with ongoing transaction.");
            } else {
                log.error("Failed: scale with ongoing transaction.");
            }
        });

        CompletableFuture<Void> statusFuture = controller.abortTransaction(stream, transaction.getTxnId());
        statusFuture.get();
        future.get();

        log.info("All scaling test PASSED");

        System.exit(0);
    }
}
