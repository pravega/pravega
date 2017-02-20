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
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.SegmentFutures;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.controller.stream.api.v1.TxnState;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.Getter;
import org.apache.thrift.TException;

/**
 * Stream controller RPC server implementation.
 */
@Getter
public class ControllerService {

    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    public ControllerService(final StreamMetadataStore streamStore,
                             final HostControllerStore hostStore,
                             final StreamMetadataTasks streamMetadataTasks,
                             final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
    }

    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig, final long createTimestamp) {
        return streamMetadataTasks.createStream(streamConfig.getScope(), streamConfig.getName(), streamConfig, createTimestamp);
    }

    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        return streamMetadataTasks.alterStream(streamConfig.getScope(), streamConfig.getName(), streamConfig);
    }

    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String stream) {
        return streamMetadataTasks.sealStream(scope, stream);
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(final String scope, final String stream) {
        // fetch active segments from segment store
        return streamStore.getActiveSegments(scope, stream)
                .thenApply(activeSegments -> activeSegments
                        .stream()
                        .map(segment -> convert(scope, stream, segment))
                        .collect(Collectors.toList())
                );
    }

    public CompletableFuture<List<Position>> getPositions(final String scope, final String stream, final long timestamp, final int count) {
        // first fetch segments active at specified timestamp from the specified stream
        // divide current segments in segmentFutures into at most count positions
        return streamStore.getActiveSegments(scope, stream, timestamp)
                .thenApply(segmentFutures -> shard(scope, stream, segmentFutures, count));
    }

    public CompletableFuture<Map<SegmentId, List<Integer>>> getSegmentsImmediatlyFollowing(SegmentId segment) {
        return streamStore.getSuccessors(segment.getScope(), segment.getStreamName(), segment.getNumber()).thenApply(successors -> {
            return successors.entrySet().stream().collect(
                    Collectors.toMap(entry -> new SegmentId(segment.getScope(), segment.getStreamName(), entry.getKey()),
                            entry -> entry.getValue()));
        });
    }

    public CompletableFuture<ScaleResponse> scale(final String scope,
                                                  final String stream,
                                                  final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final long scaleTimestamp) {
        return streamMetadataTasks.scale(scope, stream, new ArrayList<>(sealedSegments), new ArrayList<>(ModelHelper.encode(newKeyRanges)), scaleTimestamp);
    }

    public CompletableFuture<NodeUri> getURI(final SegmentId segment) throws TException {
        return CompletableFuture.completedFuture(
                SegmentHelper.getSegmentUri(segment.getScope(), segment.getStreamName(), segment.getNumber(), hostStore)
        );
    }

    private SegmentRange convert(final String scope,
                                 final String stream,
                                 final com.emc.pravega.controller.store.stream.Segment segment) {
        return new SegmentRange(
                new SegmentId(scope, stream, segment.getNumber()), segment.getKeyStart(), segment.getKeyEnd());
    }

    public CompletableFuture<Boolean> isSegmentValid(final String scope,
                                                     final String stream,
                                                     final int segmentNumber) throws TException {
        return streamStore.getActiveSegments(scope, stream)
                .thenApply(x -> x.stream().anyMatch(z -> z.getNumber() == segmentNumber));
    }

    /**
     * This method divides the current segments from the segmentFutures into at most n positions. It appropriately
     * distributes the future segments in segmentFutures among the shards. E.g., if n=5, and segmentFutures contains
     * a) 3 current segments, then 3 positions will be created each having one current segment
     * b) 6 current segments, then 5 positions will be created 1st position containing #1, #2 current segments
     * and remaining positions having 1 current segment each
     *
     * @param stream         input stream
     * @param segmentFutures input segmentFutures
     * @param n              number of shards
     * @return the list of position objects
     */
    private List<Position> shard(final String scope, final String stream, final SegmentFutures segmentFutures, final int n) {
        // divide the active segments equally into at most n partition
        int currentCount = segmentFutures.getCurrent().size();
        int quotient = currentCount / n;
        int remainder = currentCount % n;
        // if quotient < 1 then remainder number of positions shall be created, other wise n positions shall be created
        int size = (quotient < 1) ? remainder : n;
        List<Position> positions = new ArrayList<>(size);

        int counter = 0;
        // create a position object in each iteration of the for loop
        for (int i = 0; i < size; i++) {
            int j = (i < remainder) ? quotient + 1 : quotient;
            List<SegmentId> current = new ArrayList<>(j);
            for (int k = 0; k < j; k++, counter++) {
                Integer number = segmentFutures.getCurrent().get(counter);
                SegmentId segmentId = new SegmentId(scope, stream, number);
                current.add(segmentId);
            }

            // Compute the current and future segments set for position i
            Map<SegmentId, Long> currentSegments = new HashMap<>();
            current.stream().forEach(
                    x -> {
                        // TODO fetch correct offset within the segment at specified timestamp by contacting pravega host
                        // put it in the currentSegments
                        currentSegments.put(x, 0L);
                    }
            );
            // create a new position object with current segments computed
            Position position = new Position(currentSegments);
            positions.add(position);
        }
        return positions;
    }

    public CompletableFuture<TxnId> createTransaction(final String scope, final String stream) {
        return streamTransactionMetadataTasks.createTx(scope, stream).thenApply(ModelHelper::decode);
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final TxnId
            txnId) {
        return streamTransactionMetadataTasks.commitTx(scope, stream, ModelHelper.encode(txnId))
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user
                        return TxnStatus.FAILURE;
                    } else {
                        return TxnStatus.SUCCESS;
                    }
                });
    }

    public CompletableFuture<TxnStatus> abortTransaction(final String scope, final String stream, final TxnId txnId) {
        return streamTransactionMetadataTasks.abortTx(scope, stream, ModelHelper.encode(txnId))
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user
                        return TxnStatus.FAILURE;
                    } else {
                        return TxnStatus.SUCCESS;
                    }
                });
    }

    public CompletableFuture<TxnState> checkTransactionStatus(final String scope, final String stream, final TxnId
            txnId) {
        return streamStore.transactionStatus(scope, stream, ModelHelper.encode(txnId))
                .thenApply(ModelHelper::decode);
    }

    /**
     * Controller Service API to create scope.
     *
     * @param scope Name of scope to be created.
     * @return Status of create scope.
     */
    public CompletableFuture<CreateScopeStatus> createScope(final String scope) {
        return streamStore.createScope(scope);
    }

    /**
     * Controller Service API to delete scope.
     *
     * @param scope Name of scope to be deleted.
     * @return Status of delete scope.
     */
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scope) {
        return streamMetadataTasks.deleteScope(scope);
    }
}
