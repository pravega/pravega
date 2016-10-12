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
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TxId;
import com.emc.pravega.stream.impl.model.ModelHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous controller service implementation
 */
@Slf4j
public class ControllerServiceAsyncImpl implements ControllerService.AsyncIface {

    private final ControllerServiceImpl controllerService;

    public ControllerServiceAsyncImpl(StreamMetadataStore streamStore, HostControllerStore hostStore, CuratorFramework client) {
        controllerService = new ControllerServiceImpl(streamStore, hostStore, client);
    }

    @Override
    public void createStream(StreamConfig streamConfig, AsyncMethodCallback resultHandler) throws TException {
        log.debug("createStream called for stream " + streamConfig.getScope() + "/" + streamConfig.getName());
        processResult(controllerService.createStream(ModelHelper.encode(streamConfig), System.currentTimeMillis()), resultHandler);
    }

    @Override
    public void alterStream(StreamConfig streamConfig, AsyncMethodCallback resultHandler) throws TException {
        log.debug("alterStream called for stream " + streamConfig.getScope() + "/" + streamConfig.getName());
        processResult(controllerService.alterStream(ModelHelper.encode(streamConfig)), resultHandler);
    }

    @Override
    public void getCurrentSegments(String scope, String stream, AsyncMethodCallback resultHandler) throws TException {
        log.debug("getCurrentSegments called for stream " + scope + "/" + stream);
        processResult(controllerService.getCurrentSegments(scope, stream), resultHandler);
    }

    @Override
    public void getPositions(String scope, String stream, long timestamp, int count, AsyncMethodCallback resultHandler) throws TException {
        log.debug("getPositions called for stream " + scope + "/" + stream);
        processResult(controllerService.getPositions(scope, stream, timestamp, count), resultHandler);
    }

    @Override
    public void updatePositions(String scope, String stream, List<Position> positions, AsyncMethodCallback resultHandler) throws TException {
        log.debug("updatePositions called for stream " + scope + "/" + stream);
        processResult(controllerService.updatePositions(scope, stream, positions), resultHandler);
    }

    @Override
    public void scale(String scope, String stream, List<Integer> sealedSegments, Map<Double, Double> newKeyRanges, long scaleTimestamp, AsyncMethodCallback resultHandler) throws TException {
        log.debug("scale called for stream " + scope + "/" + stream);
        processResult(controllerService.scale(scope, stream, sealedSegments, newKeyRanges, scaleTimestamp), resultHandler);
    }

    @Override
    public void getURI(SegmentId segment, AsyncMethodCallback resultHandler) throws TException {
        log.debug("getURI called for segment " + segment.getScope() + "/" + segment.getStreamName() + "/" + segment.getNumber());
        processResult(controllerService.getURI(segment), resultHandler);
    }

    @Override
    public void createTransaction(String scope, String stream, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void commitTransaction(String scope, String stream, TxId txid, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void dropTransaction(String scope, String stream, TxId txid, AsyncMethodCallback resultHandler) throws TException {

    }

    @Override
    public void checkTransactionStatus(String scope, String stream, TxId txid, AsyncMethodCallback resultHandler) throws TException {

    }

    private static <T> void processResult(CompletableFuture<T> result, AsyncMethodCallback resultHandler) {
        result.whenComplete(
                (value, ex) -> {
                    log.debug("result = " + value.toString());
                    if (ex != null) {
                        resultHandler.onError(new RuntimeException(ex));
                    } else {
                        resultHandler.onComplete(value);
                    }
                });
    }
}
