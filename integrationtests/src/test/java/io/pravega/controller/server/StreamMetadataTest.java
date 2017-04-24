/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server;

import io.pravega.testcommon.TestingServerStarter;
import io.pravega.demo.ControllerWrapper;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.host.handler.PravegaConnectionListener;
import io.pravega.service.server.store.ServiceBuilder;
import io.pravega.service.server.store.ServiceBuilderConfig;
import io.pravega.stream.ScalingPolicy;
import io.pravega.stream.Segment;
import io.pravega.stream.Stream;
import io.pravega.stream.StreamConfiguration;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.StreamImpl;
import io.pravega.testcommon.TestUtils;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

import static io.pravega.testcommon.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamMetadataTest {

    @Test(timeout = 60000)
    public void testMedadataOperations() throws Exception {
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
                true,
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

        controllerWrapper.getControllerService().createScope(scope1).get();
        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
        final StreamConfiguration config1 = StreamConfiguration.builder()
                                                               .scope(scope1)
                                                               .streamName(streamName1)
                                                               .scalingPolicy(scalingPolicy)
                                                               .build();

        // create stream and seal stream

        // CS1:create a stream :given a streamName, scope and config
        assertTrue(controller.createStream(config1).get());

        // Seal a stream given a streamName and scope.
        controllerWrapper.getControllerService().createScope(scopeSeal).get();

        final StreamConfiguration configSeal = StreamConfiguration.builder()
                                                                  .scope(scopeSeal)
                                                                  .streamName(streamNameSeal)
                                                                  .scalingPolicy(scalingPolicy)
                                                                  .build();

        assertTrue(controller.createStream(configSeal).get());
        controller.getCurrentSegments(scopeSeal, streamNameSeal).get();

        assertTrue(controller.sealStream(scopeSeal, streamNameSeal).get());

        assertTrue("FAILURE: No active segments should be present in a sealed stream",
                   controller.getCurrentSegments(scopeSeal, streamNameSeal).get().getSegments().isEmpty());

        // Seal an already sealed stream.
        assertTrue(controller.sealStream(scopeSeal, streamNameSeal).get());
        assertTrue("FAILURE: No active segments should be present in a sealed stream",
                   controller.getCurrentSegments(scopeSeal, streamNameSeal).get().getSegments().isEmpty());

        assertThrows("FAILURE: Seal operation on a non-existent stream returned ",
                     controller.sealStream(scopeSeal, "nonExistentStream"),
                     t -> true);

        // CS2:stream duplication not allowed
        assertFalse(controller.createStream(config1).get());

        // CS3:create a stream with same stream name in different scopes
        controllerWrapper.getControllerService().createScope(scope2).get();

        final StreamConfiguration config2 = StreamConfiguration.builder()
                                                               .scope(scope2)
                                                               .streamName(streamName1)
                                                               .scalingPolicy(scalingPolicy)
                                                               .build();
        assertTrue(controller.createStream(config2).get());

        // CS4:create a stream with different stream name and config in same scope
        final StreamConfiguration config3 = StreamConfiguration.builder()
                                                               .scope(scope1)
                                                               .streamName(streamName2)
                                                               .scalingPolicy(ScalingPolicy.fixed(3))
                                                               .build();

        assertTrue(controller.createStream(config3).get());

        // update stream config(alter Stream)

        // AS3:update the type of scaling policy
        final StreamConfiguration config6 = StreamConfiguration.builder()
                                                               .scope(scope1)
                                                               .streamName(streamName1)
                                                               .scalingPolicy(ScalingPolicy.byDataRate(100, 2, 2))
                                                               .build();
        assertTrue(controller.alterStream(config6).get());

        // AS4:update the target rate of scaling policy
        final StreamConfiguration config7 = StreamConfiguration.builder()
                                                               .scope(scope1)
                                                               .streamName(streamName1)
                                                               .scalingPolicy(ScalingPolicy.byDataRate(200, 2, 2))
                                                               .build();
        assertTrue(controller.alterStream(config7).get());

        // AS5:update the scale factor of scaling policy
        final StreamConfiguration config8 = StreamConfiguration.builder()
                                                               .scope(scope1)
                                                               .streamName(streamName1)
                                                               .scalingPolicy(ScalingPolicy.byDataRate(200, 4, 2))
                                                               .build();
        assertTrue(controller.alterStream(config8).get());

        // AS6:update the minNumsegments of scaling policy
        final StreamConfiguration config9 = StreamConfiguration.builder()
                                                               .scope(scope1)
                                                               .streamName(streamName1)
                                                               .scalingPolicy(ScalingPolicy.byDataRate(200, 4, 3))
                                                               .build();
        assertTrue(controller.alterStream(config9).get());

        // AS7:alter configuration of non-existent stream.
        final StreamConfiguration config = StreamConfiguration.builder()
                                                              .scope("scope")
                                                              .streamName("streamName")
                                                              .scalingPolicy(ScalingPolicy.fixed(2))
                                                              .build();
        CompletableFuture<Boolean> updateStatus = controller.alterStream(config);
        assertThrows("FAILURE: Altering the configuration of a non-existent stream", updateStatus, t -> true);

        // get currently active segments

        // GCS1:get active segments of the stream
        assertFalse(controller.getCurrentSegments(scope1, streamName1).get().getSegments().isEmpty());

        // GCS2:Get active segments for a non-existent stream.

        assertThrows("Active segments cannot be fetched for non existent stream",
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
        assertThrows("Fetching segments at given time stamp for non existent stream ",
                     controller.getSegmentsAtTime(stream, System.currentTimeMillis()),
                     t -> true);

        // PS5:Get position at time before stream creation
        segments = controller.getSegmentsAtTime(stream1, System.currentTimeMillis() - 36000);
        assertEquals(controller.getCurrentSegments(scope1, streamName1).get().getSegments().size(),
                     segments.get().size());

        // PS6:Get positions at a time in future after stream creation
        segments = controller.getSegmentsAtTime(stream1, System.currentTimeMillis() + 3600);
        assertTrue(!segments.get().isEmpty());

    }
}
