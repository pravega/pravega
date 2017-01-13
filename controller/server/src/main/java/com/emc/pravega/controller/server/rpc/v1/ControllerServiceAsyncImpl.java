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
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.impl.ModelHelper;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import lombok.extern.slf4j.Slf4j;

/**
 * Asynchronous controller service implementation.
 */
@Slf4j
public class ControllerServiceAsyncImpl implements com.emc.pravega.controller.stream.api.v1.ControllerService.AsyncIface {

    private final ControllerService controllerService;

    public ControllerServiceAsyncImpl(final StreamMetadataStore streamStore,
                                      final HostControllerStore hostStore,
                                      final StreamMetadataTasks streamMetadataTasks,
                                      final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        controllerService = new ControllerService(streamStore, hostStore, streamMetadataTasks, streamTransactionMetadataTasks);
    }

    @Override
    public void createStream(final StreamConfig streamConfig, final AsyncMethodCallback resultHandler) throws TException {
        log.debug("createStream called for stream " + streamConfig.getScope() + "/" + streamConfig.getName());
        processResult(controllerService.createStream(ModelHelper.encode(streamConfig), System.currentTimeMillis()),
                resultHandler);
    }

    @Override
    public void alterStream(final StreamConfig streamConfig, final AsyncMethodCallback resultHandler) throws TException {
        log.debug("alterStream called for stream " + streamConfig.getScope() + "/" + streamConfig.getName());
        processResult(controllerService.alterStream(ModelHelper.encode(streamConfig)), resultHandler);
    }

    @Override
    public void getCurrentSegments(final String scope, final String stream, final AsyncMethodCallback resultHandler) throws TException {
        log.debug("getCurrentSegments called for stream " + scope + "/" + stream);
        processResult(controllerService.getCurrentSegments(scope, stream), resultHandler);
    }

    @Override
    public void getPositions(final String scope,
                             final String stream,
                             final long timestamp,
                             final int count,
                             final AsyncMethodCallback resultHandler) throws TException {
        log.debug("getPositions called for stream " + scope + "/" + stream);
        processResult(controllerService.getPositions(scope, stream, timestamp, count), resultHandler);
    }
    
    @Override
    public void getAvailableFutureSegments(Position position, List<Position> otherPositions,
            AsyncMethodCallback resultHandler) throws TException {
        log.debug("getAvailableFutureSegments called for position " + position);
        processResult(controllerService.getAvailableFutureSegments(position, otherPositions), resultHandler);
    }

    @Override
    public void scale(final String scope,
                      final String stream,
                      final List<Integer> sealedSegments,
                      final Map<Double, Double> newKeyRanges,
                      final long scaleTimestamp,
                      final AsyncMethodCallback resultHandler) throws TException {
        log.debug("scale called for stream " + scope + "/" + stream);
        processResult(controllerService.scale(scope, stream, sealedSegments, newKeyRanges, scaleTimestamp), resultHandler);
    }

    @Override
    public void getURI(final SegmentId segment, final AsyncMethodCallback resultHandler) throws TException {
        log.debug("getURI called for segment " + segment.getScope() + "/" + segment.getStreamName() + "/" + segment.getNumber());
        processResult(controllerService.getURI(segment), resultHandler);
    }

    @Override
    public void isSegmentValid(final String scope,
                               final String stream,
                               final int segmentNumber,
                               final AsyncMethodCallback resultHandler) throws TException {
        log.debug("isSegmentValid called for stream " + scope + "/" + stream + " segment " + segmentNumber);
        processResult(controllerService.isSegmentValid(scope, stream, segmentNumber), resultHandler);
    }

    @Override
    public void createTransaction(final String scope,
                                  final String stream,
                                  final AsyncMethodCallback resultHandler) throws TException {
        log.debug("createTransaction called for stream " + scope + "/" + stream);
        processResult(controllerService.createTransaction(scope, stream), resultHandler);
    }

    @Override
    public void commitTransaction(final String scope,
                                  final String stream,
                                  final TxnId txid,
                                  final AsyncMethodCallback resultHandler) throws TException {
        log.debug("commitTransaction called for stream " + scope + "/" + stream + " txid=" + txid);
        processResult(controllerService.commitTransaction(scope, stream, txid), resultHandler);
    }

    @Override
    public void dropTransaction(final String scope,
                                final String stream,
                                final TxnId txid,
                                final AsyncMethodCallback resultHandler) throws TException {
        log.debug("dropTransaction called for stream " + scope + "/" + stream + " txid=" + txid);
        processResult(controllerService.dropTransaction(scope, stream, txid), resultHandler);
    }

    @Override
    public void checkTransactionStatus(final String scope,
                                       final String stream,
                                       final TxnId txid,
                                       final AsyncMethodCallback resultHandler) throws TException {
        log.debug("checkTransactionStatus called for stream " + scope + "/" + stream + " txid=" + txid);
        processResult(controllerService.checkTransactionStatus(scope, stream, txid), resultHandler);
    }

    private static <T> void processResult(final CompletableFuture<T> result, final AsyncMethodCallback resultHandler) {
        result.whenComplete(
                (value, ex) -> {
                    log.debug("result = " + (value == null ? "null" : value.toString()));

                    if (ex != null) {
                        resultHandler.onError(new RuntimeException(ex));
                    } else if (value != null) {
                        resultHandler.onComplete(value);
                    }
                });
    }
}
