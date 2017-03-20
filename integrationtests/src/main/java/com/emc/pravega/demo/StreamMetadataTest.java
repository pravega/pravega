/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.demo;

import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public class StreamMetadataTest {
    @SuppressWarnings("checkstyle:ReturnCount")
    public static void main(String[] args) throws Exception {
        @Cleanup
        TestingServer zkTestServer = new TestingServer();

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        ControllerWrapper controllerWrapper = new ControllerWrapper(zkTestServer.getConnectString());
        Controller controller = controllerWrapper.getController();

        final String scope1 = "scope1";
        controllerWrapper.getControllerService().createScope("scope1").get();

        final String streamName1 = "stream1";
        final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(2);
        final StreamConfiguration config1 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName1)
                .scalingPolicy(scalingPolicy)
                .build();
        CompletableFuture<CreateStreamStatus> createStatus;

        //create stream and seal stream

        //CS1:create a stream :given a streamName, scope and config
        System.err.println(String.format("Creating stream (%s, %s)", scope1, streamName1));
        createStatus = controller.createStream(config1);
        if (createStatus.get().getStatus() != CreateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Create stream failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Stream created");
        }

        //Seal a stream given a streamName and scope.
        final String scopeSeal = "scopeSeal";
        controllerWrapper.getControllerService().createScope("scopeSeal").get();

        final String streamNameSeal = "streamSeal";
        final StreamConfiguration configSeal = StreamConfiguration.builder()
                .scope(scopeSeal)
                .streamName(streamNameSeal)
                .scalingPolicy(scalingPolicy)
                .build();
        System.err.println(String.format("Seal stream  (%s, %s)", scopeSeal, streamNameSeal));
        CreateStreamStatus createStream3Status = controller.createStream(configSeal).get();
        if ( createStream3Status.getStatus() != CreateStreamStatus.Status.SUCCESS) {
           System.err.println("FAILURE: Create stream operation failed");
        }
        @SuppressWarnings("unused")
        StreamSegments result = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
        UpdateStreamStatus sealStatus = controller.sealStream(scopeSeal, streamNameSeal).get();
        if (sealStatus.getStatus() == UpdateStreamStatus.Status.SUCCESS) {
            System.err.println("SUCCESS: Stream Sealed");
            StreamSegments currentSegs = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
            if (!currentSegs.getSegments().isEmpty()) {
                System.err.println("FAILURE: No active segments should be present in a sealed stream");
            }
        } else {
            System.err.println("FAILURE: Seal stream failed, exiting");
            return;
        }

        //Seal an already sealed stream.
        UpdateStreamStatus reSealStatus = controller.sealStream(scopeSeal, streamNameSeal).get();
        if (reSealStatus.getStatus() == UpdateStreamStatus.Status.SUCCESS) {
            StreamSegments currentSegs = controller.getCurrentSegments(scopeSeal, streamNameSeal).get();
            if (!currentSegs.getSegments().isEmpty()) {
                System.err.println("FAILURE: No active segments should be present in a sealed stream");
            }
        } else {
            System.err.println("FAILURE: Seal operation on an already sealed stream failed, exiting");
            return;
        }

        //Seal a non-existent stream.
        UpdateStreamStatus errSealStatus = controller.sealStream(scopeSeal, "nonExistentStream").get();
        if (errSealStatus.getStatus() != UpdateStreamStatus.Status.STREAM_NOT_FOUND) {
            System.err.println("FAILURE: Seal operation on a non-existent stream returned " + errSealStatus);
        }

        //CS2:stream duplication not allowed
        System.err.println(String.format("Duplicating stream (%s, %s)", scope1, streamName1));
        createStatus = controller.createStream(config1);
        if (createStatus.get().getStatus() == CreateStreamStatus.Status.STREAM_EXISTS) {
            System.err.println("SUCCESS: Stream duplication not allowed ");
        } else if (createStatus.get().getStatus() == CreateStreamStatus.Status.FAILURE) {
            System.err.println("FAILURE: Create stream failed, exiting");
            return;
        } else {
            System.err.println("FAILURE: Stream duplication successful, exiting");
            return;
        }

        //CS3:create a stream with same stream name in different scopes
        final String scope2 = "scope2";
        controllerWrapper.getControllerService().createScope("scope2").get();

        final StreamConfiguration config2 = StreamConfiguration.builder()
                .scope(scope2)
                .streamName(streamName1)
                .scalingPolicy(scalingPolicy)
                .build();
        System.err.println(String.format("Creating stream with same stream name (%s) in different scope (%s)", scope2, streamName1));
        createStatus = controller.createStream(config2);
        if (createStatus.get().getStatus() != CreateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Creating stream with same stream name in different scope failed, exiting ");
            return;
        } else {
            System.err.println("SUCCESS: Stream created with same stream name in different scope");
        }

        //CS4:create a stream with different stream name and config  in same scope
        final String streamName2 = "stream2";
        final StreamConfiguration config3 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName2)
                .scalingPolicy(ScalingPolicy.fixed(3))
                .build();
        System.err.println(String.format("Creating stream with different stream  name (%s) and config in same scope (%s)", scope1, streamName2));
        createStatus = controller.createStream(config3);
        if (createStatus.get().getStatus() != CreateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Create stream  with different stream name and config  in same scope failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Stream created  with different stream name and config  in same scope ");
        }

        //update stream config(alter Stream)

        //AS1:update the stream name
        final StreamConfiguration config4 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName("stream4")
                .scalingPolicy(scalingPolicy)
                .build();
        CompletableFuture<UpdateStreamStatus> updateStatus;
        updateStatus = controller.alterStream(config4);
        System.err.println(String.format("Updating the stream name (%s, %s)", scope1, "stream4"));
        if (updateStatus.get().getStatus() != UpdateStreamStatus.Status.STREAM_NOT_FOUND) {
            System.err.println("FAILURE: Stream name updated, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Stream name cannot be  updated");
        }

        //AS3:update the type of scaling policy
        final StreamConfiguration config6 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName1)
                .scalingPolicy(ScalingPolicy.byDataRate(100, 2, 2))
                .build();
        updateStatus = controller.alterStream(config6);
        System.err.println(String.format("Updating the  type of scaling policy(%s, %s)", scope1, streamName1));
        if (updateStatus.get().getStatus() != UpdateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Update the  type of scaling policy failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Updated the Scaling policy type ");
        }

        //AS4:update the target rate of scaling policy
        final StreamConfiguration config7 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName1)
                .scalingPolicy(ScalingPolicy.byDataRate(200, 2, 2))
                .build();
        System.err.println(String.format("Updating the target rate (%s, %s)", scope1, streamName1));
        updateStatus = controller.alterStream(config7);
        if (updateStatus.get().getStatus() != UpdateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Update the target rate failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Updated the target rate of scaling policy");
        }

        //AS5:update the scale factor of scaling policy
        final StreamConfiguration config8 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName1)
                .scalingPolicy(ScalingPolicy.byDataRate(200, 4, 2))
                .build();
        System.err.println(String.format("Updating the scalefactor (%s, %s)", scope1, streamName1));
        updateStatus = controller.alterStream(config8);
        if (updateStatus.get().getStatus() != UpdateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Update the scalefactor failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Updated the scalefactor of scaling policy");
        }

        //AS6:update the minNumsegments of scaling policy
        final StreamConfiguration config9 = StreamConfiguration.builder()
                .scope(scope1)
                .streamName(streamName1)
                .scalingPolicy(ScalingPolicy.byDataRate(200, 4, 3))
                .build();
        System.err.println(String.format("Updating the min Num segments (%s, %s)", scope1, streamName1));
        updateStatus = controller.alterStream(config9);
        if (updateStatus.get().getStatus() != UpdateStreamStatus.Status.SUCCESS) {
            System.err.println("FAILURE: Update  min Num segments failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Updated the min Num segments of scaling policy");
        }

        //AS7:alter configuration of non-existent stream.
        final StreamConfiguration config = StreamConfiguration.builder()
                .scope("scope")
                .streamName("streamName")
                .scalingPolicy(ScalingPolicy.fixed(2))
                .build();
        System.err.println(String.format("Altering the  configuration of a non-existent stream (%s, %s)", "scope", "streamName"));
        updateStatus = controller.alterStream(config);
        if (updateStatus.get().getStatus() ==  UpdateStreamStatus.Status.STREAM_NOT_FOUND) {
            System.err.println("SUCCESS: Altering the configuration of a non-existent stream is not allowed");
        } else if (updateStatus.get().getStatus() == UpdateStreamStatus.Status.FAILURE) {
            System.err.println("FAILURE: Alter configuration failed, exiting");
            return;
        } else {
            System.err.println("FAILURE: Altering the configuration of a non-existent stream, exiting");
            return;
        }

        //get currently active segments

        //GCS1:get active segments of the stream
        CompletableFuture<StreamSegments> getActiveSegments;
        System.err.println(String.format("Get active segments of the stream (%s, %s)", scope1, streamName1));
        getActiveSegments = controller.getCurrentSegments(scope1, streamName1);
        if (getActiveSegments.get().getSegments().isEmpty()) {
            System.err.println("FAILURE: Fetching active segments failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Fetching active segments");
        }

        //GCS2:Get active segments for a non-existent stream.
        System.err.println(String.format("Get active segments of the non existent stream (%s, %s)", "scope", "streamName"));
        try {
            getActiveSegments = controller.getCurrentSegments("scope", "streamName");
            if (getActiveSegments.get().getSegments().isEmpty()) {
                System.err.println("SUCCESS: Active segments cannot be fetched for non existent stream");
            } else {
                System.err.println("FAILURE: Fetching active segments for non existent stream, exiting ");
                return;
            }
        } catch (ExecutionException | CompletionException e) {
            if (!(e.getCause() instanceof DataNotFoundException)) {
                System.err.println("FAILURE: Fetching active segments for non existent stream, exiting ");
                return;
            } else {
                System.err.println("SUCCESS: Active segments cannot be fetched for non existent stream");
            }
        }

        //get  positions at a given time stamp

        //PS1:get  positions at a given time stamp:given stream, time stamp, count
        Stream stream1 = new StreamImpl(scope1, streamName1);
        final int count1 = 10;
        CompletableFuture<List<PositionInternal>> getPositions;
        System.err.println(String.format("Fetching positions at given time stamp of (%s,%s)", scope1, streamName1));
        getPositions = controller.getPositions(stream1, System.currentTimeMillis(), count1);
        if (getPositions.get().isEmpty()) {
            System.err.println("FAILURE: Fetching positions at given time stamp failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Fetching positions at given time stamp");
        }

        //PS2:get positions of a stream with different count
        final int count2 = 20;
        System.err.println(String.format("Positions at given time stamp (%s, %s)", scope1, streamName1));
        getPositions = controller.getPositions(stream1, System.currentTimeMillis(), count2);
        if (getPositions.get().isEmpty()) {
            System.err.println("FAILURE: Fetching positions at given time stamp with different count failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Fetching positions at given time stamp with different count");
        }

        //PS3:get positions of a different stream at a given time stamp
        Stream stream2 = new StreamImpl(scope1, streamName2);
        System.err.println(String.format("Fetching positions at given time stamp of (%s, %s)", scope1, stream2));
        getPositions = controller.getPositions(stream2, System.currentTimeMillis(), count1);
        if (getPositions.get().isEmpty()) {
            System.err.println("FAILURE: Fetching positions at given time stamp for a different stream of same scope failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Fetching positions at given time stamp for a different stream of same scope");
        }

        //PS4:get positions at a given timestamp for non-existent stream.
        Stream stream = new StreamImpl("scope", "streamName");
        System.err.println(String.format("Fetching positions at given time stamp for non existent stream (%s, %s)", "scope", "streamName"));
        try {
            getPositions = controller.getPositions(stream, System.currentTimeMillis(), count1);
            if (getPositions.get().isEmpty()) {
                System.err.println("SUCCESS: Positions cannot be fetched for non existent stream");
            } else {
                System.err.println("FAILURE: Fetching positions for non existent stream, exiting ");
                return;
            }
        } catch (ExecutionException | CompletionException e) {
            if (!(e.getCause() instanceof DataNotFoundException)) {
                System.err.println("FAILURE: Fetching positions for non existent stream, exiting ");
                return;
            } else {
                System.err.println("SUCCESS: Positions cannot be fetched for non existent stream");
            }
        }

        //PS5:Get position at time before stream creation
        System.err.println(String.format("Get positions at time before (%s, %s) creation ", scope1, streamName1));
        getPositions = controller.getPositions(stream1, System.currentTimeMillis() - 36000, count1);
        if (getPositions.get().size() == controller.getCurrentSegments(scope1, streamName1).get().getSegments().size()) {
            System.err.println("SUCCESS: Fetching positions at given time before stream creation");
        } else {
            System.err.println("FAILURE: Fetching positions at given time before stream creation failed, exiting");
            System.exit(1);
        }

        //PS6:Get positions at a time in future after stream creation
        System.err.println(String.format("Get positions at given time in future after (%s, %s) creation ", scope1, streamName1));
        getPositions = controller.getPositions(stream1, System.currentTimeMillis() + 3600, count1);
        if (getPositions.get().isEmpty()) {
            System.err.println("FAILURE: Fetching positions at given time in furture after stream creation failed, exiting");
            return;
        } else {
            System.err.println("SUCCESS: Fetching positions at given time in furture after stream creation");
        }

        System.out.println("All stream metadata tests PASSED");

        System.exit(0);

    }
}
