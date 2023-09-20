/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.integration.controller.server;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.ControllerWrapper;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertFutureThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamMetadataTest {

    @Test(timeout = 60000)
    public void testMetadataOperations() throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServerStarter().start();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        int servicePort = TestUtils.getAvailableListenPort();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, servicePort, store, tableStore,
                serviceBuilder.getLowPriorityExecutor(), new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store));
        server.startListening();
        int controllerPort = TestUtils.getAvailableListenPort();
        @Cleanup
        ControllerWrapper controllerWrapper = new ControllerWrapper(
                zkTestServer.getConnectString(),
                false,
                controllerPort,
                "localhost",
                servicePort,
                4);
        Controller controller = controllerWrapper.getController();

        final String scope1 = "scope1";
        final String streamName1 = "stream1";
        final String scopeSeal = "scopeSeal";
        final String streamNameSeal = "streamSeal";
        final String scope2 = "scope2";
        final String streamName2 = "stream2";

        assertEquals(CreateScopeStatus.Status.SUCCESS,
                     controllerWrapper.getControllerService().createScope(scope1, 0L).get().getStatus());
        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
        final StreamConfiguration config1 = StreamConfiguration.builder()
                                                               .scalingPolicy(scalingPolicy)
                                                               .build();

        // create stream and seal stream

        // CS1:create a stream :given a streamName, scope and config
        assertTrue(controller.createStream(scope1, streamName1, config1).get());

        // Seal a stream given a streamName and scope.
        controllerWrapper.getControllerService().createScope(scopeSeal, 0L).get();

        final StreamConfiguration configSeal = StreamConfiguration.builder()
                                                                  .scalingPolicy(scalingPolicy)
                                                                  .build();

        assertTrue(controller.createStream(scopeSeal, streamNameSeal, configSeal).get());
        controller.getCurrentSegments(scopeSeal, streamNameSeal).get();

        assertTrue(controller.sealStream(scopeSeal, streamNameSeal).get());

        assertTrue("FAILURE: No active segments should be present in a sealed stream",
                   controller.getCurrentSegments(scopeSeal, streamNameSeal).get().getSegments().isEmpty());

        // Seal an already sealed stream.
        assertTrue(controller.sealStream(scopeSeal, streamNameSeal).get());
        assertTrue("FAILURE: No active segments should be present in a sealed stream",
                   controller.getCurrentSegments(scopeSeal, streamNameSeal).get().getSegments().isEmpty());

        assertFutureThrows("FAILURE: Seal operation on a non-existent stream returned ",
                     controller.sealStream(scopeSeal, "nonExistentStream"),
                     t -> true);

        // CS2:stream duplication not allowed
        assertFalse(controller.createStream(scope1, streamName1, config1).get());

        // CS3:create a stream with same stream name in different scopes
        controllerWrapper.getControllerService().createScope(scope2, 0L).get();

        final StreamConfiguration config2 = StreamConfiguration.builder()
                                                               .scalingPolicy(scalingPolicy)
                                                               .build();
        assertTrue(controller.createStream(scope2, streamName1, config2).get());

        // CS4:create a stream with different stream name and config in same scope
        final StreamConfiguration config3 = StreamConfiguration.builder()
                                                               .scalingPolicy(ScalingPolicy.fixed(3))
                                                               .build();

        assertTrue(controller.createStream(scope1, streamName2, config3).get());

        // update stream config(update Stream)

        // AS3:update the type of scaling policy
        final StreamConfiguration config6 = StreamConfiguration.builder()
                                                               .scalingPolicy(ScalingPolicy.byDataRate(100, 2, 2))
                                                               .build();
        assertTrue(controller.updateStream(scope1, streamName1, config6).get());

        // AS4:update the target rate of scaling policy
        final StreamConfiguration config7 = StreamConfiguration.builder()
                                                               .scalingPolicy(ScalingPolicy.byDataRate(200, 2, 2))
                                                               .build();
        assertTrue(controller.updateStream(scope1, streamName1, config7).get());

        // AS5:update the scale factor of scaling policy
        final StreamConfiguration config8 = StreamConfiguration.builder()
                                                               .scalingPolicy(ScalingPolicy.byDataRate(200, 4, 2))
                                                               .build();
        assertTrue(controller.updateStream(scope1, streamName1, config8).get());

        // AS6:update the minNumsegments of scaling policy
        final StreamConfiguration config9 = StreamConfiguration.builder()
                                                               .scalingPolicy(ScalingPolicy.byDataRate(200, 4, 3))
                                                               .build();
        assertTrue(controller.updateStream(scope1, streamName1, config9).get());

        // the number of segments in the stream should now be 3. 
        
        // AS7:Update configuration of non-existent stream.
        final StreamConfiguration config = StreamConfiguration.builder()
                                                              .scalingPolicy(ScalingPolicy.fixed(2))
                                                              .build();
        CompletableFuture<Boolean> updateStatus = controller.updateStream("scope", "streamName", config);
        assertFutureThrows("FAILURE: Updating the configuration of a non-existent stream", updateStatus, t -> true);

        // get currently active segments

        // GCS1:get active segments of the stream
        assertFalse(controller.getCurrentSegments(scope1, streamName1).get().getSegments().isEmpty());

        // GCS2:Get active segments for a non-existent stream.

        assertFutureThrows("Active segments cannot be fetched for non existent stream",
                     controller.getCurrentSegments("scope", "streamName"),
                     t -> true);

        // get positions at a given time stamp

        // PS1:get positions at a given time stamp:given stream, time stamp, count
        Stream stream1 = new StreamImpl(scope1, streamName1);
        CompletableFuture<Map<Segment, Long>> segments = controller.getSegmentsAtTime(stream1,
                                                                                      System.currentTimeMillis());
        assertEquals(2, segments.get().size());

        // PS2:get positions of a stream with different count
        Stream stream2 = new StreamImpl(scope1, streamName2);
        segments = controller.getSegmentsAtTime(stream2, System.currentTimeMillis());
        assertEquals(3, segments.get().size());

        // PS4:get positions at a given timestamp for non-existent stream.
        Stream stream = new StreamImpl("scope", "streamName");
        assertFutureThrows("Fetching segments at given time stamp for non existent stream ",
                     controller.getSegmentsAtTime(stream, System.currentTimeMillis()),
                     t -> true);

        // PS5:Get position at time before stream creation
        segments = controller.getSegmentsAtTime(stream1, System.currentTimeMillis() - 36000);
        assertEquals(segments.join().size(), 2);

        assertEquals(controller.getCurrentSegments(scope1, streamName1).get().getSegments().size(),
                     3);

        // PS6:Get positions at a time in future after stream creation
        segments = controller.getSegmentsAtTime(stream1, System.currentTimeMillis() + 3600);
        assertTrue(!segments.get().isEmpty());

    }
}
