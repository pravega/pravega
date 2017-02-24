/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.StreamConfig;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.stream.impl.ModelHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous controller service implementation.
 */
@Slf4j
public class ControllerServiceAsyncImpl implements com.emc.pravega.controller.stream.api.v1.ControllerService.AsyncIface {

    private final ControllerService controllerService;

    public ControllerServiceAsyncImpl(final ControllerService controllerService) {
        this.controllerService = controllerService;
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
    public void sealStream(String scope, String stream, AsyncMethodCallback resultHandler) throws TException {
        log.debug("sealStream called for stream {}", stream);
        processResult(controllerService.sealStream(scope, stream), resultHandler);
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
    public void getSegmentsImmediatlyFollowing(SegmentId segment, AsyncMethodCallback resultHandler) throws TException {
        log.debug("getSegmentsImmediatlyFollowing called for segment " + segment);
        processResult(controllerService.getSegmentsImmediatlyFollowing(segment), resultHandler);
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
    public void abortTransaction(final String scope,
                                 final String stream,
                                 final TxnId txid,
                                 final AsyncMethodCallback resultHandler) throws TException {
        log.debug("abortTransaction called for stream " + scope + "/" + stream + " txid=" + txid);
        processResult(controllerService.abortTransaction(scope, stream, txid), resultHandler);
    }

    @Override
    public void checkTransactionStatus(final String scope,
                                       final String stream,
                                       final TxnId txid,
                                       final AsyncMethodCallback resultHandler) throws TException {
        log.debug("checkTransactionStatus called for stream " + scope + "/" + stream + " txid=" + txid);
        processResult(controllerService.checkTransactionStatus(scope, stream, txid), resultHandler);
    }

    /**
     * Controller Service Async API to create scope.
     *
     * @param scope         Name of scope to be created.
     * @param resultHandler callback result handler
     * @throws TException exception class for Thrift.
     */
    @Override
    public void createScope(final String scope, final AsyncMethodCallback resultHandler) throws TException {
        log.debug("createScope called for scope {}", scope);
        processResult(controllerService.createScope(scope), resultHandler);
    }

    /**
     * Controller Service Async API to delete scope.
     *
     * @param scope         Name of scope to be deleted.
     * @param resultHandler callback result handler
     * @throws TException exception class for Thrift.
     */
    @Override
    public void deleteScope(final String scope, final AsyncMethodCallback resultHandler) throws TException {
        log.debug("deleteScope called for scope {}", scope);
        processResult(controllerService.deleteScope(scope), resultHandler);
    }

    private static <T> void processResult(final CompletableFuture<T> result, final AsyncMethodCallback resultHandler) {
        result.whenComplete(
                (value, ex) -> {
                    log.debug("result = " + (value == null ? "null" : value.toString()));

                    if (ex != null) {
                        // OnError from Thrift callback has a bug. So we are completing with null
                        // so that client gets an exception. Ideally we want to let the client know
                        // of the exact error but at least with this we wont have client stuck waiting for
                        // a response from server.
                        resultHandler.onComplete(null);
                    } else if (value != null) {
                        resultHandler.onComplete(value);
                    }
                });
    }

}
