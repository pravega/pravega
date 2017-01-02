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

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TransactionStatus;
import com.emc.pravega.controller.stream.api.v1.TxId;
import com.emc.pravega.controller.stream.api.v1.TxState;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.ModelHelper;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Synchronous controller service implementation.
 */
public class ControllerServiceSyncImpl implements com.emc.pravega.controller.stream.api.v1.ControllerService.Iface {

    private final ControllerService controllerService;

    public ControllerServiceSyncImpl(final StreamMetadataStore streamStore,
                                     final HostControllerStore hostStore,
                                     final StreamMetadataTasks streamMetadataTasks,
                                     final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
    }

    /**
     * Create the stream metadata in the metadata streamStore.
     * Start with creation of minimum number of segments.
     * Asynchronously call createSegment on pravega hosts notifying them about new segments in the stream.
     */
    @Override
    public CreateStreamStatus createStream(final StreamConfig streamConfig) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createStream(ModelHelper.encode(streamConfig),
                System.currentTimeMillis()), RuntimeException::new);
    }

    @Override
    public UpdateStreamStatus alterStream(final StreamConfig streamConfig) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public UpdateStreamStatus sealStream(String scope, String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.sealStream(scope, stream), RuntimeException::new);
    }

    @Override
    public List<SegmentRange> getCurrentSegments(final String scope, final String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getCurrentSegments(scope, stream), RuntimeException::new);
    }

    @Override
    public NodeUri getURI(final SegmentId segment) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getURI(segment), RuntimeException::new);
    }

    @Override
    public boolean isSegmentValid(final String scope, final String stream, final int segmentNumber) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.isSegmentValid(scope, stream, segmentNumber), RuntimeException::new);
    }

    @Override
    public List<Position> getPositions(final String scope, final String stream, final long timestamp, final int count) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getPositions(scope, stream, timestamp, count), RuntimeException::new);
    }

    @Override
    public List<Position> updatePositions(final String scope, final String stream, final List<Position> positions) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.updatePositions(scope, stream, positions), RuntimeException::new);
    }

    @Override
    public ScaleResponse scale(final String scope, final String stream, final List<Integer> sealedSegments, final Map<Double, Double> newKeyRanges, final long scaleTimestamp) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.scale(scope, stream, sealedSegments, newKeyRanges, scaleTimestamp), RuntimeException::new);
    }

    @Override
    public TxId createTransaction(final String scope, final String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createTransaction(scope, stream), RuntimeException::new);
    }

    @Override
    public TransactionStatus commitTransaction(final String scope, final String stream, final TxId txid) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.commitTransaction(scope, stream, txid), RuntimeException::new);
    }

    @Override
    public TransactionStatus dropTransaction(final String scope, final String stream, final TxId txid) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.dropTransaction(scope, stream, txid), RuntimeException::new);
    }

    @Override
    public TxState checkTransactionStatus(final String scope, final String stream, final TxId txid) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.checkTransactionStatus(scope, stream, txid), RuntimeException::new);
    }
}
