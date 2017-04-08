/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.rpc.grpc.v1;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime.SegmentLocation;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import com.emc.pravega.stream.impl.ModelHelper;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC Service API implementation for the Controller.
 */
@Slf4j
@AllArgsConstructor
public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    // The underlying Controller Service implementation to delegate all API calls to.
    private final ControllerService controllerService;

    @Override
    public void createStream(StreamConfig request, StreamObserver<CreateStreamStatus> responseObserver) {
        log.info("createStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        processResult(controllerService.createStream(ModelHelper.encode(request), System.currentTimeMillis()),
                      responseObserver);
    }

    @Override
    public void alterStream(StreamConfig request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("alterStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        processResult(controllerService.alterStream(ModelHelper.encode(request)), responseObserver);
    }

    @Override
    public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("sealStream called for stream {}/{}.", request.getScope(), request.getStream());
        processResult(controllerService.sealStream(request.getScope(), request.getStream()), responseObserver);
    }

    @Override
    public void deleteStream(StreamInfo request, StreamObserver<DeleteStreamStatus> responseObserver) {
        log.info("deleteStream called for stream {}/{}.", request.getScope(), request.getStream());
        processResult(controllerService.deleteStream(request.getScope(), request.getStream()), responseObserver);
    }

    @Override
    public void getCurrentSegments(StreamInfo request, StreamObserver<SegmentRanges> responseObserver) {
        log.info("getCurrentSegments called for stream {}/{}.", request.getScope(), request.getStream());
        processResult(controllerService.getCurrentSegments(request.getScope(), request.getStream())
                              .thenApply(segmentRanges -> SegmentRanges.newBuilder()
                                      .addAllSegmentRanges(segmentRanges)
                                      .build()),
                      responseObserver);
    }

    @Override
    public void getSegments(GetSegmentsRequest request, StreamObserver<SegmentsAtTime> responseObserver) {
        log.debug("getSegments called for stream " + request.getStreamInfo().getScope() + "/" +
                          request.getStreamInfo().getStream());
        processResult(controllerService.getSegmentsAtTime(request.getStreamInfo().getScope(),
                                                          request.getStreamInfo().getStream(),
                                                          request.getTimestamp())
                                       .thenApply(segments -> {
                                           SegmentsAtTime.Builder builder = SegmentsAtTime.newBuilder();
                                           for (Entry<SegmentId, Long> entry : segments.entrySet()) {
                                               builder.addSegments(SegmentLocation.newBuilder()
                                                                                  .setSegmentId(entry.getKey())
                                                                                  .setOffset(entry.getValue())
                                                                                  .build());
                                           }
                                           return builder.build();
                                       }),
                      responseObserver);
    }

    @Override
    public void getSegmentsImmediatlyFollowing(SegmentId segmentId,
            StreamObserver<SuccessorResponse> responseObserver) {
        log.info("getSegmentsImmediatelyFollowing called for segment {} ", segmentId);
        processResult(controllerService.getSegmentsImmediatelyFollowing(segmentId)
                              .thenApply(ModelHelper::createSuccessorResponse),
                      responseObserver);
    }

    @Override
    public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
        log.info("scale called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        processResult(controllerService.scale(request.getStreamInfo().getScope(),
                                              request.getStreamInfo().getStream(),
                                              request.getSealedSegmentsList(),
                                              request.getNewKeyRangesList().stream().collect(Collectors.toMap(
                                                      entry -> entry.getStart(), entry -> entry.getEnd())),
                                              request.getScaleTimestamp()),
                      responseObserver);
    }

    @Override
    public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
        log.info("getURI called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentNumber());
        processResult(controllerService.getURI(request), responseObserver);
    }

    @Override
    public void isSegmentValid(SegmentId request,
            StreamObserver<SegmentValidityResponse> responseObserver) {
        log.info("isSegmentValid called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentNumber());
        processResult(controllerService.isSegmentValid(request.getStreamInfo().getScope(),
                                                       request.getStreamInfo().getStream(),
                                                       request.getSegmentNumber())
                .thenApply(bRes -> SegmentValidityResponse.newBuilder().setResponse(bRes).build()),
                      responseObserver);
    }

    @Override
    public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
        log.info("createTransaction called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        processResult(controllerService.createTransaction(request.getStreamInfo().getScope(),
                                                          request.getStreamInfo().getStream(),
                                                          request.getLease(),
                                                          request.getMaxExecutionTime(),
                                                          request.getScaleGracePeriod())
                .thenApply(pair -> Controller.CreateTxnResponse.newBuilder()
                        .setTxnId(ModelHelper.decode(pair.getKey()))
                        .addAllActiveSegments(pair.getValue())
                        .build()),
                      responseObserver);
    }

    @Override
    public void commitTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("commitTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        processResult(controllerService.commitTransaction(request.getStreamInfo().getScope(),
                                                          request.getStreamInfo().getStream(),
                                                          request.getTxnId()),
                      responseObserver);
    }

    @Override
    public void abortTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("abortTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        processResult(controllerService.abortTransaction(request.getStreamInfo().getScope(),
                                                        request.getStreamInfo().getStream(),
                                                        request.getTxnId()),
                      responseObserver);
    }

    @Override
    public void pingTransaction(PingTxnRequest request, StreamObserver<PingTxnStatus> responseObserver) {
        processResult(controllerService.pingTransaction(request.getStreamInfo().getScope(),
                                                        request.getStreamInfo().getStream(),
                                                        request.getTxnId(),
                                                        request.getLease()),
                      responseObserver);
    }

    @Override
    public void checkTransactionState(TxnRequest request, StreamObserver<TxnState> responseObserver) {
        log.info("checkTransactionState called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        processResult(controllerService.checkTransactionStatus(request.getStreamInfo().getScope(),
                                                               request.getStreamInfo().getStream(),
                                                               request.getTxnId()),
                      responseObserver);
    }

    @Override
    public void createScope(ScopeInfo request,
            StreamObserver<CreateScopeStatus> responseObserver) {
        log.info("createScope called for scope {}.", request.getScope());
        processResult(controllerService.createScope(request.getScope()),
                      responseObserver);
    }

    @Override
    public void deleteScope(ScopeInfo request,
            StreamObserver<DeleteScopeStatus> responseObserver) {
        log.info("deleteScope called for scope {}.", request.getScope());
        processResult(controllerService.deleteScope(request.getScope()),
                      responseObserver);
    }

    // Convert responses from CompletableFuture to gRPC's Observer pattern.
    private static <T> void processResult(final CompletableFuture<T> result, final StreamObserver<T> streamObserver) {
        result.whenComplete(
                (value, ex) -> {
                    log.debug("result = " + (value == null ? "null" : value.toString()));

                    if (ex != null) {
                        Throwable cause = ExceptionHelpers.getRealException(ex);
                        log.warn("Controller api failed with error: {}", cause.getMessage());
                        streamObserver.onError(Status.INTERNAL
                                .withCause(cause)
                                .withDescription(cause.getMessage())
                                .asRuntimeException());
                    } else if (value != null) {
                        streamObserver.onNext(value);
                        streamObserver.onCompleted();
                    }
                });
    }
}
