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
import com.emc.pravega.controller.stream.api.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TxId;
import com.emc.pravega.controller.stream.api.v1.TxStatus;
import com.emc.pravega.stream.impl.model.ModelHelper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

import java.util.List;

/**
 * Synchronous controller service implementation
 */
public class ControllerServiceSyncImpl implements ControllerService.Iface {

    private final ControllerServiceImpl controllerService;

    public ControllerServiceSyncImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        controllerService = new ControllerServiceImpl(streamStore, hostStore);
    }

    /**
     * Create the stream metadata in the metadata streamStore.
     * Start with creation of minimum number of segments.
     * Asynchronously call createSegment on pravega hosts notifying them about new segments in the stream.
     */
    @Override
    public Status createStream(StreamConfig streamConfig) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.createStream(ModelHelper.encode(streamConfig)), RuntimeException::new);
    }

    @Override
    public Status alterStream(StreamConfig streamConfig) throws TException {
        throw new NotImplementedException();
    }

    @Override
    public List<SegmentRange> getCurrentSegments(String scope, String stream) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getCurrentSegments(scope, stream), RuntimeException::new);
    }

    @Override
    public NodeUri getURI(SegmentId segment) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getURI(segment), RuntimeException::new);
    }

    @Override
    public List<Position> getPositions(String scope, String stream, long timestamp, int count) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.getPositions(scope, stream, timestamp, count), RuntimeException::new);
    }

    @Override
    public List<Position> updatePositions(String scope, String stream, List<Position> positions) throws TException {
        return FutureHelpers.getAndHandleExceptions(controllerService.updatePositions(scope, stream, positions), RuntimeException::new);
    }

    @Override
    public TxId createTransaction(String scope, String stream) throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Status commitTransaction(String scope, String stream, TxId txid) throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Status dropTransaction(String scope, String stream, TxId txid) throws TException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TxStatus checkTransactionStatus(String scope, String stream, TxId txid) throws TException {
        // TODO Auto-generated method stub
        return null;
    }


}
