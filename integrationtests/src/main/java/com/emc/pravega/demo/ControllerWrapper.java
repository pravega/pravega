/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.demo;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.store.StoreClient;
import com.emc.pravega.controller.store.ZKStoreClient;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostStoreFactory;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.ZKStreamMetadataStore;
import com.emc.pravega.controller.store.task.TaskMetadataStore;
import com.emc.pravega.controller.store.task.TaskStoreFactory;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamSegments;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.thrift.TException;

public class ControllerWrapper implements Controller {

    private final ControllerService controller;

    public ControllerWrapper(String connectionString) {
        String hostId;
        try {
            // On each controller process restart, it gets a fresh hostId,
            // which is a combination of hostname and random GUID.
            hostId = InetAddress.getLocalHost().getHostAddress() + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            hostId = UUID.randomUUID().toString();
        }

        // initialize the executor service
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(20,
                new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

        CuratorFramework client = CuratorFrameworkFactory.newClient(connectionString, new RetryOneTime(2000));
        client.start();

        StoreClient storeClient = new ZKStoreClient(client);

        StreamMetadataStore streamStore = new ZKStreamMetadataStore(client, executor);

        HostControllerStore hostStore = HostStoreFactory.createStore(HostStoreFactory.StoreType.InMemory);

        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createStore(storeClient, executor);

        //2) start RPC server with v1 implementation. Enable other versions if required.
        StreamMetadataTasks streamMetadataTasks = new StreamMetadataTasks(streamStore, hostStore, taskMetadataStore,
                executor, hostId);
        StreamTransactionMetadataTasks streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore,
                hostStore, taskMetadataStore, executor, hostId);

        controller = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
    }

    @Override
    public CompletableFuture<CreateScopeStatus> createScope(String scope) {
        return controller.createScope(scope);
    }

    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScope(String scope) {
        return controller.deleteScope(scope);
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(StreamConfiguration streamConfig) {
        return controller.createStream(streamConfig, System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(StreamConfiguration streamConfig) {
        return controller.alterStream(streamConfig);
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> sealStream(String scope, String streamName) {
        return controller.sealStream(scope, streamName);
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                        final List<Integer> sealedSegments,
                                                        final Map<Double, Double> newKeyRanges) {
        return controller.scale(stream.getScope(),
                stream.getStreamName(),
                sealedSegments,
                newKeyRanges,
                System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String scope, String stream) {
        return controller.getCurrentSegments(scope, stream)
                .thenApply((List<SegmentRange> ranges) -> {
                    NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
                    for (SegmentRange r : ranges) {
                        rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
                    }
                    return rangeMap;
                })
                .thenApply(StreamSegments::new);
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txId) {
        return FutureHelpers.toVoidExpecting(controller.commitTransaction(stream.getScope(),
                                                                          stream.getStreamName(),
                                                                          ModelHelper.decode(txId)),
                                             TxnStatus.SUCCESS,
                                             TxnFailedException::new);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        return FutureHelpers.toVoidExpecting(controller.abortTransaction(stream.getScope(),
                                                                        stream.getStreamName(),
                                                                        ModelHelper.decode(txId)),
                                             TxnStatus.SUCCESS,
                                             TxnFailedException::new);
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txnId) {
        return controller.checkTransactionStatus(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txnId))
                .thenApply(status -> ModelHelper.encode(status, stream + " " + txnId));
    }

    @Override
    public CompletableFuture<UUID> createTransaction(Stream stream, long timeout) {
        return controller.createTransaction(stream.getScope(), stream.getStreamName())
                .thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count) {
        return controller.getPositions(stream.getScope(), stream.getStreamName(), timestamp, count)
                .thenApply(result -> result.stream().map(ModelHelper::encode).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(final Segment segment) {
        return controller.getSegmentsImmediatlyFollowing(ModelHelper.decode(segment)).thenApply(successors -> {
            Map<Segment, List<Integer>> result = new HashMap<>();
            for (Entry<SegmentId, List<Integer>> successor : successors.entrySet()) {
                result.put(ModelHelper.encode(successor.getKey()), successor.getValue());
            }
            return result;
        });
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        Segment segment = Segment.fromScopedName(qualifiedSegmentName);
        try {
            return controller.getURI(new SegmentId(segment.getScope(), segment.getStreamName(),
                    segment.getSegmentNumber())).thenApply(ModelHelper::encode);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

}

