package io.pravega.controller.stream.api.grpc.v1;

import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;

/**
 * <pre>
 * Producer, Consumer and Admin APIs supported by Stream Controller Service
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.2.0)",
    comments = "Source: Controller.proto")
public final class ControllerServiceGrpc {

  private ControllerServiceGrpc() {}

  public static final String SERVICE_NAME = "io.pravega.controller.stream.api.grpc.v1.ControllerService";

  // Static method descriptors that strictly reflect the proto.
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse> METHOD_GET_CONTROLLER_SERVER_LIST =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "getControllerServerList"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig,
      io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus> METHOD_CREATE_STREAM =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "createStream"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig,
      io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> METHOD_ALTER_STREAM =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "alterStream"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo,
      io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> METHOD_SEAL_STREAM =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "sealStream"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo,
      io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus> METHOD_DELETE_STREAM =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "deleteStream"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo,
      io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges> METHOD_GET_CURRENT_SEGMENTS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "getCurrentSegments"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime> METHOD_GET_SEGMENTS =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "getSegments"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId,
      io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse> METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "getSegmentsImmediatlyFollowing"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse> METHOD_SCALE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "scale"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId,
      io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri> METHOD_GET_URI =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "getURI"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId,
      io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse> METHOD_IS_SEGMENT_VALID =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "isSegmentValid"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse> METHOD_CREATE_TRANSACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "createTransaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> METHOD_COMMIT_TRANSACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "commitTransaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> METHOD_ABORT_TRANSACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "abortTransaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus> METHOD_PING_TRANSACTION =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "pingTransaction"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest,
      io.pravega.controller.stream.api.grpc.v1.Controller.TxnState> METHOD_CHECK_TRANSACTION_STATE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "checkTransactionState"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.TxnState.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo,
      io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus> METHOD_CREATE_SCOPE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "createScope"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus.getDefaultInstance()));
  @io.grpc.ExperimentalApi("https://github.com/grpc/grpc-java/issues/1901")
  public static final io.grpc.MethodDescriptor<io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo,
      io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus> METHOD_DELETE_SCOPE =
      io.grpc.MethodDescriptor.create(
          io.grpc.MethodDescriptor.MethodType.UNARY,
          generateFullMethodName(
              "io.pravega.controller.stream.api.grpc.v1.ControllerService", "deleteScope"),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo.getDefaultInstance()),
          io.grpc.protobuf.ProtoUtils.marshaller(io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus.getDefaultInstance()));

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ControllerServiceStub newStub(io.grpc.Channel channel) {
    return new ControllerServiceStub(channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ControllerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    return new ControllerServiceBlockingStub(channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary and streaming output calls on the service
   */
  public static ControllerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    return new ControllerServiceFutureStub(channel);
  }

  /**
   * <pre>
   * Producer, Consumer and Admin APIs supported by Stream Controller Service
   * </pre>
   */
  public static abstract class ControllerServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getControllerServerList(io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CONTROLLER_SERVER_LIST, responseObserver);
    }

    /**
     */
    public void createStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_STREAM, responseObserver);
    }

    /**
     */
    public void alterStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ALTER_STREAM, responseObserver);
    }

    /**
     */
    public void sealStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SEAL_STREAM, responseObserver);
    }

    /**
     */
    public void deleteStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_STREAM, responseObserver);
    }

    /**
     */
    public void getCurrentSegments(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_CURRENT_SEGMENTS, responseObserver);
    }

    /**
     */
    public void getSegments(io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_SEGMENTS, responseObserver);
    }

    /**
     */
    public void getSegmentsImmediatlyFollowing(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING, responseObserver);
    }

    /**
     */
    public void scale(io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_SCALE, responseObserver);
    }

    /**
     */
    public void getURI(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_GET_URI, responseObserver);
    }

    /**
     */
    public void isSegmentValid(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_IS_SEGMENT_VALID, responseObserver);
    }

    /**
     */
    public void createTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_TRANSACTION, responseObserver);
    }

    /**
     */
    public void commitTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_COMMIT_TRANSACTION, responseObserver);
    }

    /**
     */
    public void abortTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_ABORT_TRANSACTION, responseObserver);
    }

    /**
     */
    public void pingTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_PING_TRANSACTION, responseObserver);
    }

    /**
     */
    public void checkTransactionState(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnState> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CHECK_TRANSACTION_STATE, responseObserver);
    }

    /**
     */
    public void createScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_CREATE_SCOPE, responseObserver);
    }

    /**
     */
    public void deleteScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus> responseObserver) {
      asyncUnimplementedUnaryCall(METHOD_DELETE_SCOPE, responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            METHOD_GET_CONTROLLER_SERVER_LIST,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse>(
                  this, METHODID_GET_CONTROLLER_SERVER_LIST)))
          .addMethod(
            METHOD_CREATE_STREAM,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig,
                io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus>(
                  this, METHODID_CREATE_STREAM)))
          .addMethod(
            METHOD_ALTER_STREAM,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig,
                io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus>(
                  this, METHODID_ALTER_STREAM)))
          .addMethod(
            METHOD_SEAL_STREAM,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo,
                io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus>(
                  this, METHODID_SEAL_STREAM)))
          .addMethod(
            METHOD_DELETE_STREAM,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo,
                io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus>(
                  this, METHODID_DELETE_STREAM)))
          .addMethod(
            METHOD_GET_CURRENT_SEGMENTS,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo,
                io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges>(
                  this, METHODID_GET_CURRENT_SEGMENTS)))
          .addMethod(
            METHOD_GET_SEGMENTS,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime>(
                  this, METHODID_GET_SEGMENTS)))
          .addMethod(
            METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId,
                io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse>(
                  this, METHODID_GET_SEGMENTS_IMMEDIATLY_FOLLOWING)))
          .addMethod(
            METHOD_SCALE,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse>(
                  this, METHODID_SCALE)))
          .addMethod(
            METHOD_GET_URI,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId,
                io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri>(
                  this, METHODID_GET_URI)))
          .addMethod(
            METHOD_IS_SEGMENT_VALID,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId,
                io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse>(
                  this, METHODID_IS_SEGMENT_VALID)))
          .addMethod(
            METHOD_CREATE_TRANSACTION,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse>(
                  this, METHODID_CREATE_TRANSACTION)))
          .addMethod(
            METHOD_COMMIT_TRANSACTION,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus>(
                  this, METHODID_COMMIT_TRANSACTION)))
          .addMethod(
            METHOD_ABORT_TRANSACTION,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus>(
                  this, METHODID_ABORT_TRANSACTION)))
          .addMethod(
            METHOD_PING_TRANSACTION,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus>(
                  this, METHODID_PING_TRANSACTION)))
          .addMethod(
            METHOD_CHECK_TRANSACTION_STATE,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest,
                io.pravega.controller.stream.api.grpc.v1.Controller.TxnState>(
                  this, METHODID_CHECK_TRANSACTION_STATE)))
          .addMethod(
            METHOD_CREATE_SCOPE,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo,
                io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus>(
                  this, METHODID_CREATE_SCOPE)))
          .addMethod(
            METHOD_DELETE_SCOPE,
            asyncUnaryCall(
              new MethodHandlers<
                io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo,
                io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus>(
                  this, METHODID_DELETE_SCOPE)))
          .build();
    }
  }

  /**
   * <pre>
   * Producer, Consumer and Admin APIs supported by Stream Controller Service
   * </pre>
   */
  public static final class ControllerServiceStub extends io.grpc.stub.AbstractStub<ControllerServiceStub> {
    private ControllerServiceStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ControllerServiceStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerServiceStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ControllerServiceStub(channel, callOptions);
    }

    /**
     */
    public void getControllerServerList(io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CONTROLLER_SERVER_LIST, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_STREAM, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void alterStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ALTER_STREAM, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sealStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SEAL_STREAM, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_STREAM, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getCurrentSegments(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_CURRENT_SEGMENTS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSegments(io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_SEGMENTS, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSegmentsImmediatlyFollowing(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void scale(io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_SCALE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getURI(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_GET_URI, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void isSegmentValid(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_IS_SEGMENT_VALID, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_TRANSACTION, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commitTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_COMMIT_TRANSACTION, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void abortTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_ABORT_TRANSACTION, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pingTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_PING_TRANSACTION, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void checkTransactionState(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnState> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CHECK_TRANSACTION_STATE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_CREATE_SCOPE, getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void deleteScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request,
        io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(METHOD_DELETE_SCOPE, getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Producer, Consumer and Admin APIs supported by Stream Controller Service
   * </pre>
   */
  public static final class ControllerServiceBlockingStub extends io.grpc.stub.AbstractStub<ControllerServiceBlockingStub> {
    private ControllerServiceBlockingStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ControllerServiceBlockingStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerServiceBlockingStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ControllerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse getControllerServerList(io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CONTROLLER_SERVER_LIST, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus createStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_STREAM, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus alterStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ALTER_STREAM, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus sealStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SEAL_STREAM, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus deleteStream(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_STREAM, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges getCurrentSegments(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_CURRENT_SEGMENTS, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime getSegments(io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_SEGMENTS, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse getSegmentsImmediatlyFollowing(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse scale(io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_SCALE, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri getURI(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request) {
      return blockingUnaryCall(
          getChannel(), METHOD_GET_URI, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse isSegmentValid(io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request) {
      return blockingUnaryCall(
          getChannel(), METHOD_IS_SEGMENT_VALID, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse createTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_TRANSACTION, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus commitTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_COMMIT_TRANSACTION, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus abortTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_ABORT_TRANSACTION, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus pingTransaction(io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_PING_TRANSACTION, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.TxnState checkTransactionState(io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CHECK_TRANSACTION_STATE, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus createScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request) {
      return blockingUnaryCall(
          getChannel(), METHOD_CREATE_SCOPE, getCallOptions(), request);
    }

    /**
     */
    public io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus deleteScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request) {
      return blockingUnaryCall(
          getChannel(), METHOD_DELETE_SCOPE, getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Producer, Consumer and Admin APIs supported by Stream Controller Service
   * </pre>
   */
  public static final class ControllerServiceFutureStub extends io.grpc.stub.AbstractStub<ControllerServiceFutureStub> {
    private ControllerServiceFutureStub(io.grpc.Channel channel) {
      super(channel);
    }

    private ControllerServiceFutureStub(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ControllerServiceFutureStub build(io.grpc.Channel channel,
        io.grpc.CallOptions callOptions) {
      return new ControllerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse> getControllerServerList(
        io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CONTROLLER_SERVER_LIST, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus> createStream(
        io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_STREAM, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> alterStream(
        io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ALTER_STREAM, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus> sealStream(
        io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SEAL_STREAM, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus> deleteStream(
        io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_STREAM, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges> getCurrentSegments(
        io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_CURRENT_SEGMENTS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime> getSegments(
        io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_SEGMENTS, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse> getSegmentsImmediatlyFollowing(
        io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse> scale(
        io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_SCALE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri> getURI(
        io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_GET_URI, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse> isSegmentValid(
        io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_IS_SEGMENT_VALID, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse> createTransaction(
        io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_TRANSACTION, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> commitTransaction(
        io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_COMMIT_TRANSACTION, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus> abortTransaction(
        io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_ABORT_TRANSACTION, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus> pingTransaction(
        io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_PING_TRANSACTION, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.TxnState> checkTransactionState(
        io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CHECK_TRANSACTION_STATE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus> createScope(
        io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_CREATE_SCOPE, getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus> deleteScope(
        io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request) {
      return futureUnaryCall(
          getChannel().newCall(METHOD_DELETE_SCOPE, getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_CONTROLLER_SERVER_LIST = 0;
  private static final int METHODID_CREATE_STREAM = 1;
  private static final int METHODID_ALTER_STREAM = 2;
  private static final int METHODID_SEAL_STREAM = 3;
  private static final int METHODID_DELETE_STREAM = 4;
  private static final int METHODID_GET_CURRENT_SEGMENTS = 5;
  private static final int METHODID_GET_SEGMENTS = 6;
  private static final int METHODID_GET_SEGMENTS_IMMEDIATLY_FOLLOWING = 7;
  private static final int METHODID_SCALE = 8;
  private static final int METHODID_GET_URI = 9;
  private static final int METHODID_IS_SEGMENT_VALID = 10;
  private static final int METHODID_CREATE_TRANSACTION = 11;
  private static final int METHODID_COMMIT_TRANSACTION = 12;
  private static final int METHODID_ABORT_TRANSACTION = 13;
  private static final int METHODID_PING_TRANSACTION = 14;
  private static final int METHODID_CHECK_TRANSACTION_STATE = 15;
  private static final int METHODID_CREATE_SCOPE = 16;
  private static final int METHODID_DELETE_SCOPE = 17;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final ControllerServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(ControllerServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_CONTROLLER_SERVER_LIST:
          serviceImpl.getControllerServerList((io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse>) responseObserver);
          break;
        case METHODID_CREATE_STREAM:
          serviceImpl.createStream((io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus>) responseObserver);
          break;
        case METHODID_ALTER_STREAM:
          serviceImpl.alterStream((io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus>) responseObserver);
          break;
        case METHODID_SEAL_STREAM:
          serviceImpl.sealStream((io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus>) responseObserver);
          break;
        case METHODID_DELETE_STREAM:
          serviceImpl.deleteStream((io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus>) responseObserver);
          break;
        case METHODID_GET_CURRENT_SEGMENTS:
          serviceImpl.getCurrentSegments((io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges>) responseObserver);
          break;
        case METHODID_GET_SEGMENTS:
          serviceImpl.getSegments((io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime>) responseObserver);
          break;
        case METHODID_GET_SEGMENTS_IMMEDIATLY_FOLLOWING:
          serviceImpl.getSegmentsImmediatlyFollowing((io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse>) responseObserver);
          break;
        case METHODID_SCALE:
          serviceImpl.scale((io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse>) responseObserver);
          break;
        case METHODID_GET_URI:
          serviceImpl.getURI((io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri>) responseObserver);
          break;
        case METHODID_IS_SEGMENT_VALID:
          serviceImpl.isSegmentValid((io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse>) responseObserver);
          break;
        case METHODID_CREATE_TRANSACTION:
          serviceImpl.createTransaction((io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnResponse>) responseObserver);
          break;
        case METHODID_COMMIT_TRANSACTION:
          serviceImpl.commitTransaction((io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus>) responseObserver);
          break;
        case METHODID_ABORT_TRANSACTION:
          serviceImpl.abortTransaction((io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus>) responseObserver);
          break;
        case METHODID_PING_TRANSACTION:
          serviceImpl.pingTransaction((io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus>) responseObserver);
          break;
        case METHODID_CHECK_TRANSACTION_STATE:
          serviceImpl.checkTransactionState((io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.TxnState>) responseObserver);
          break;
        case METHODID_CREATE_SCOPE:
          serviceImpl.createScope((io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus>) responseObserver);
          break;
        case METHODID_DELETE_SCOPE:
          serviceImpl.deleteScope((io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo) request,
              (io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static final class ControllerServiceDescriptorSupplier implements io.grpc.protobuf.ProtoFileDescriptorSupplier {
    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.pravega.controller.stream.api.grpc.v1.Controller.getDescriptor();
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ControllerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ControllerServiceDescriptorSupplier())
              .addMethod(METHOD_GET_CONTROLLER_SERVER_LIST)
              .addMethod(METHOD_CREATE_STREAM)
              .addMethod(METHOD_ALTER_STREAM)
              .addMethod(METHOD_SEAL_STREAM)
              .addMethod(METHOD_DELETE_STREAM)
              .addMethod(METHOD_GET_CURRENT_SEGMENTS)
              .addMethod(METHOD_GET_SEGMENTS)
              .addMethod(METHOD_GET_SEGMENTS_IMMEDIATLY_FOLLOWING)
              .addMethod(METHOD_SCALE)
              .addMethod(METHOD_GET_URI)
              .addMethod(METHOD_IS_SEGMENT_VALID)
              .addMethod(METHOD_CREATE_TRANSACTION)
              .addMethod(METHOD_COMMIT_TRANSACTION)
              .addMethod(METHOD_ABORT_TRANSACTION)
              .addMethod(METHOD_PING_TRANSACTION)
              .addMethod(METHOD_CHECK_TRANSACTION_STATE)
              .addMethod(METHOD_CREATE_SCOPE)
              .addMethod(METHOD_DELETE_SCOPE)
              .build();
        }
      }
    }
    return result;
  }
}
