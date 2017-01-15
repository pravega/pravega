/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.server;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamSegments;
import org.apache.commons.lang.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class LocalController implements Controller {

    private ControllerService controller;

    public LocalController(final StreamMetadataStore streamStore,
                    final HostControllerStore hostStore,
                    final StreamMetadataTasks streamMetadataTasks,
                    final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        this.controller = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig) {
        return this.controller.createStream(streamConfig, System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        return this.controller.alterStream(streamConfig);
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                        final List<Integer> sealedSegments,
                                                        final Map<Double, Double> newKeyRanges) {
        return this.controller.scale(stream.getScope(),
                stream.getStreamName(),
                sealedSegments,
                newKeyRanges,
                System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName) {
        final NavigableMap<Double, Segment> segments = new TreeMap<>();
        return this.controller
                .getCurrentSegments(scope, streamName)
                .thenApply(x -> {
                    x.stream()
                            .forEach(segmentRange ->
                                    segments.put(
                                            segmentRange.getMaxKey(),
                                            new Segment(scope,
                                                    streamName,
                                                    segmentRange.getSegmentId().getNumber())));
                    return new StreamSegments(segments);
                });
    }

    @Override
    public CompletableFuture<UUID> createTransaction(Stream stream, long timeout) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TransactionStatus> commitTransaction(Stream stream, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<TransactionStatus> dropTransaction(Stream stream, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<List<PositionInternal>> updatePositions(Stream stream, List<PositionInternal> positions) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Boolean> isSegmentValid(String scope, String stream, int segmentNumber) {
        throw new NotImplementedException();
    }
}