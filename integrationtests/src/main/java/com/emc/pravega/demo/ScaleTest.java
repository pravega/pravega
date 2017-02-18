/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.curator.test.TestingServer;

import lombok.Cleanup;

/**
 * End to end scale tests.
 *
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

        Controller controller = ControllerWrapper.getController(zkTestServer.getConnectString());

        // Create controller object for testing against a separate controller process.
        // ControllerImpl controller = new ControllerImpl("localhost", 9090);

        final String scope = "scope";
        final String streamName = "stream1";
        final StreamConfiguration config =
                new StreamConfigurationImpl(scope,
                        streamName,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 0L, 0, 1));

        Stream stream = new StreamImpl(scope, streamName);

        System.err.println(String.format("Creating stream (%s, %s)", scope, streamName));
        CompletableFuture<CreateStreamStatus> createStatus = controller.createStream(config);
        if (createStatus.get() != CreateStreamStatus.SUCCESS) {
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
        CompletableFuture<UUID> txIdFuture = controller.createTransaction(stream, 60000, 600000, 30000);
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
        if (scaleResponse.getStatus() != ScaleStreamStatus.PRECONDITION_FAILED) {
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
