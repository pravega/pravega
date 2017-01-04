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
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.emc.pravega.stream.impl.StreamImpl;
import com.emc.pravega.stream.impl.StreamSegments;
import lombok.Cleanup;
import org.apache.curator.test.TestingServer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StreamMetadataTest {
    @SuppressWarnings("checkstyle:ReturnCount")
    public static void main(String[] args) throws Exception {
        TestingServer zkTestServer = new TestingServer();
        ControllerWrapper controller = new ControllerWrapper(zkTestServer.getConnectString());

        ServiceBuilder serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        serviceBuilder.initialize().get();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, 12345, store);
        server.startListening();

        final String scope1 = "scope1";
        final String streamName1 = "stream1";
        final StreamConfiguration config1 =
                new StreamConfigurationImpl(scope1,
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
        CompletableFuture<CreateStreamStatus> createStatus;

        //create stream
        System.err.println(String.format("Creating stream (%s, %s)", scope1, streamName1));
        createStatus = controller.createStream(config1);
        if (createStatus.get() != CreateStreamStatus.SUCCESS) {
            System.err.println("Create stream failed, exiting");
            return;
        }

        //stream duplication not allowed
        System.err.println(String.format("duplicating stream (%s, %s)", scope1, streamName1));
        createStatus = controller.createStream(config1);
        if (createStatus.get() == CreateStreamStatus.STREAM_EXISTS) {
            System.err.println("stream duplication not allowed ");
        } else if (createStatus.get() == CreateStreamStatus.FAILURE) {
            System.err.println("create stream failed, exiting");
            return;
        }

        //same stream name in different scopes--> fails --> #250 bug have to be fixed
        final String scope2 = "scope2";
        final StreamConfiguration config2 =
                new StreamConfigurationImpl(scope2,
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
        System.err.println(String.format("creating stream with same stream name in different scope (%s, %s)", scope2, streamName1));
        createStatus = controller.createStream(config2);
        if (createStatus.get() != CreateStreamStatus.SUCCESS) {
            System.err.println("Create stream failed, exiting ");
            return;
        }

        //different stream name and config  in same scope
        final String streamName2 = "stream2";
        final StreamConfiguration config3 =
                new StreamConfigurationImpl(scope1,
                        streamName2,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3));
        System.err.println(String.format("creating stream with different stream  name and config in same scope(%s, %s)", scope1, streamName2));
        createStatus = controller.createStream(config3);
        if (createStatus.get() != CreateStreamStatus.SUCCESS) {
            System.err.println("Create stream failed, exiting");
            return;
        }

        //update stream config
        //updating the stream name
        final StreamConfiguration config4 =
                new StreamConfigurationImpl(scope1,
                        "stream4",
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
        CompletableFuture<UpdateStreamStatus> updateStatus = controller.alterStream(config4);
        System.err.println(String.format("updtaing the stream name (%s, %s)", scope1, "stream4"));
        if (updateStatus.get() != UpdateStreamStatus.STREAM_NOT_FOUND) {
            System.err.println(" Stream name cannot be updated, exiting");
            return;
        }

        //updating the scope name
        final StreamConfiguration config5 =
                new StreamConfigurationImpl("scope5",
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 2));
        updateStatus = controller.alterStream(config5);
        System.err.println(String.format("updtaing the scope name (%s, %s)", "scope5", streamName1));
        if (updateStatus.get() != UpdateStreamStatus.SUCCESS) {
            System.err.println("Update stream config failed, exiting");
            return;
        }

        //change the type of scaling policy
        final StreamConfiguration config6 =
                new StreamConfigurationImpl(scope1,
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_BYTES, 100L, 2, 2));
        updateStatus = controller.alterStream(config6);
        System.err.println(String.format("updating the scaling policy type(%s, %s)", scope1, streamName1));
        if (updateStatus.get() != UpdateStreamStatus.SUCCESS) {
            System.err.println("Update stream config failed, exiting");
            return;
        }
        //change the target rate of scaling policy
        final StreamConfiguration config7 =
                new StreamConfigurationImpl(scope1,
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 200L, 2, 2));
        System.err.println(String.format("updating the target rate (%s, %s)", scope1, streamName1));
        updateStatus = controller.alterStream(config7);
        if (updateStatus.get() != UpdateStreamStatus.SUCCESS) {
            System.err.println("Update stream config failed, exiting");
            return;
        }

        //change the scalefactor of scaling policy
        final StreamConfiguration config8 =
                new StreamConfigurationImpl(scope1,
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 3, 2));
        System.err.println(String.format("updating the scalefactor (%s, %s)", scope1, streamName1));
        updateStatus = controller.alterStream(config8);
        if (updateStatus.get() != UpdateStreamStatus.SUCCESS) {
            System.err.println("Update stream config failed, exiting");
            return;
        }

        //change the minNumsegments
        final StreamConfiguration config9 =
                new StreamConfigurationImpl(scope1,
                        streamName1,
                        new ScalingPolicy(ScalingPolicy.Type.FIXED_NUM_SEGMENTS, 100L, 2, 3));

        System.err.println(String.format("updating the min Num segments (%s, %s)", scope1, streamName1));
        CompletableFuture<UpdateStreamStatus> updateStatus4 = controller.alterStream(config9);
        if (updateStatus4.get() != UpdateStreamStatus.SUCCESS) {
            System.err.println("Update stream config failed, exiting");
            return;
        }

        //get active segments of the stream
        CompletableFuture<StreamSegments> getActiveSegments;
        System.err.println(String.format("get active segments of the stream (%s, %s)", scope1, streamName1));
        getActiveSegments = controller.getCurrentSegments(scope1, streamName1);
        if (getActiveSegments.get().getSegments().isEmpty()) {
           System.err.println("fetching active segments failed, exiting");
           return;
        }

        //get current position at a given time stamp
        Stream stream1 = new StreamImpl(scope1, streamName1, config1);
        final int count1 = 10;
        CompletableFuture<List<PositionInternal>> currentPosition;
        System.err.println(String.format("position at given time stamp (%s,%s)", scope1, streamName1));
        currentPosition  =  controller.getPositions(stream1, System.currentTimeMillis(), count1);
        if (currentPosition.get().isEmpty()) {
            System.err.println("fetching position at given time stamp failed, exiting");
            return;
        }

        final int count2 = 20;
        System.err.println(String.format("position at given time stamp (%s, %s)", scope1, streamName1));
        currentPosition  =  controller.getPositions(stream1, System.currentTimeMillis(), count2);
        if (currentPosition.get().isEmpty()) {
            System.err.println("fetching position at given time stamp failed, exiting");
            return;
        }

        Stream stream2 = new StreamImpl(scope1, streamName2, config3);
        System.err.println(String.format("position at given time stamp (%s, %s)", scope1, stream2));
        currentPosition  =  controller.getPositions(stream2, System.currentTimeMillis(), count1);
        if (currentPosition.get().isEmpty()) {
            System.err.println("fetching position at given time stamp failed, exiting");
            return;
        }

    }
}
