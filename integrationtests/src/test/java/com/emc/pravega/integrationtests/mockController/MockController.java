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

package com.emc.pravega.integrationtests.mockController;

import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.StreamSegments;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockController {

    public static MockAdmin getAdmin(String endpoint, int port) {
        return new MockAdmin(endpoint, port);
    }

    public static MockProducer getProducer(String endpoint, int port) {
        return new MockProducer(endpoint, port);
    }

    public static MockConsumer getConsumer(String endpoint, int port) {
        return new MockConsumer(endpoint, port);
    }

    @AllArgsConstructor
    private static class MockAdmin implements ControllerApi.Admin {
        private final String endpoint;
        private final int port;

        @Override
        public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
            SegmentId segmentId = new SegmentId(streamConfig.getName(), streamConfig.getName(), 0, -1);

            com.emc.pravega.stream.impl.segment.SegmentHelper.createSegment(segmentId.getQualifiedName(), new SegmentUri(endpoint, port));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return CompletableFuture.completedFuture(Status.SUCCESS);
        }

        @Override
        public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
            return null;
        }
    }

    @AllArgsConstructor
    private static class MockProducer implements ControllerApi.Producer {

        private final String endpoint;
        private final int port;

        @Override
        public CompletableFuture<StreamSegments> getCurrentSegments(String stream) {
            return CompletableFuture.completedFuture(new StreamSegments(
                    Lists.newArrayList(new SegmentId(stream, stream, 0, -1)),
                    System.currentTimeMillis()));
        }

        @Override
        public CompletableFuture<SegmentUri> getURI(String stream, SegmentId id) {
            return CompletableFuture.completedFuture(new SegmentUri(endpoint, port));
        }
    }

    @AllArgsConstructor
    private static class MockConsumer implements ControllerApi.Consumer {
        private final String endpoint;
        private final int port;

        @Override
        public CompletableFuture<List<PositionInternal>> getPositions(String stream, long timestamp, int count) {
            return null;
        }

        @Override
        public CompletableFuture<List<PositionInternal>> updatePositions(String stream, List<PositionInternal> positions) {
            return null;
        }

        @Override
        public CompletableFuture<SegmentUri> getURI(String stream, SegmentId id) {
            return CompletableFuture.completedFuture(new SegmentUri(endpoint, port));
        }
    }
}
