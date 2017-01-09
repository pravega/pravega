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
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamSegments;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.thrift.TException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class ControllerWrapper implements Controller {

    private final ControllerService controller;

    public ControllerWrapper(String connectionString) {
        String hostId;
        try {
            // On each controller incoming restart, it gets a fresh hostId,
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
    public CompletableFuture<CreateStreamStatus> createStream(StreamConfiguration streamConfig) {
        return controller.createStream(streamConfig, System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(StreamConfiguration streamConfig) {
        return controller.alterStream(streamConfig);
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
    public CompletableFuture<TransactionStatus> commitTransaction(Stream stream, UUID txId) {
        return controller.commitTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId));
    }

    @Override
    public CompletableFuture<TransactionStatus> dropTransaction(Stream stream, UUID txId) {
        return controller.dropTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId));
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txId) {
        return controller.checkTransactionStatus(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId))
                .thenApply(status -> ModelHelper.encode(status, stream + " " + txId));
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
    public CompletableFuture<List<PositionInternal>> updatePositions(Stream stream, List<PositionInternal> positions) {
        final List<com.emc.pravega.controller.stream.api.v1.Position> transformed =
                positions.stream().map(ModelHelper::decode).collect(Collectors.toList());

        return controller.updatePositions(stream.getScope(), stream.getStreamName(), transformed)
                .thenApply(result -> result.stream().map(ModelHelper::encode).collect(Collectors.toList()));
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

    @Override
    public CompletableFuture<Boolean> isSegmentValid(String scope, String stream, int segmentNumber) {
        try {
            return controller.isSegmentValid(scope, stream, segmentNumber);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}

