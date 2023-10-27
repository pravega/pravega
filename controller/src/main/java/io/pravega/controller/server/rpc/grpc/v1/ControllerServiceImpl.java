/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.server.rpc.grpc.v1;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthenticationException;
import io.pravega.auth.AuthorizationException;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.server.security.auth.StreamAuthParams;
import io.pravega.controller.server.security.auth.handler.AuthContext;
import io.pravega.controller.store.stream.StoreException;
import io.pravega.controller.store.task.LockFailedException;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateKeyValueTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateReaderGroupResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.DelegationToken;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteKVTableStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteReaderGroupStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetEpochSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.KeyValueTableInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfigResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupConfiguration;
import io.pravega.controller.stream.api.grpc.v1.Controller.ReaderGroupInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime.SegmentLocation;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamCut;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscriberStreamCut;
import io.pravega.controller.stream.api.grpc.v1.Controller.SubscribersResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateReaderGroupResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateSubscriberStatus;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.AccessOperation;
import io.pravega.shared.security.auth.AuthorizationResource;
import io.pravega.shared.security.auth.AuthorizationResourceImpl;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.pravega.client.control.impl.ModelHelper.decode;
import static io.pravega.shared.controller.tracing.RPCTracingTags.ABORT_TRANSACTION;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CHECK_SCALE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CHECK_SCOPE_EXISTS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CHECK_STREAM_EXISTS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CHECK_TRANSACTION_STATE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.COMMIT_TRANSACTION;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CREATE_KEY_VALUE_TABLE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CREATE_READER_GROUP;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CREATE_SCOPE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CREATE_STREAM;
import static io.pravega.shared.controller.tracing.RPCTracingTags.CREATE_TRANSACTION;
import static io.pravega.shared.controller.tracing.RPCTracingTags.DELETE_KEY_VALUE_TABLE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.DELETE_READER_GROUP;
import static io.pravega.shared.controller.tracing.RPCTracingTags.DELETE_SCOPE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.DELETE_SCOPE_RECURSIVE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.DELETE_STREAM;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_CURRENT_SEGMENTS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_CURRENT_SEGMENTS_KEY_VALUE_TABLE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_EPOCH_SEGMENTS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_KEY_VALUE_TABLE_CONFIGURATION;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_OR_REFRESH_DELEGATION_TOKEN_FOR;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_READER_GROUP_CONFIG;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_SEGMENTS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_SEGMENTS_BETWEEN_STREAM_CUTS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_SEGMENTS_IMMEDIATELY_FOLLOWING;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_STREAM_CONFIGURATION;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_URI;
import static io.pravega.shared.controller.tracing.RPCTracingTags.IS_SEGMENT_OPEN;
import static io.pravega.shared.controller.tracing.RPCTracingTags.IS_STREAMCUT_VALID;
import static io.pravega.shared.controller.tracing.RPCTracingTags.LIST_KEY_VALUE_TABLES;
import static io.pravega.shared.controller.tracing.RPCTracingTags.LIST_SCOPES;
import static io.pravega.shared.controller.tracing.RPCTracingTags.LIST_STREAMS_IN_SCOPE;
import static io.pravega.shared.controller.tracing.RPCTracingTags.LIST_STREAMS_IN_SCOPE_FOR_TAG;
import static io.pravega.shared.controller.tracing.RPCTracingTags.LIST_SUBSCRIBERS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.NOTE_TIMESTAMP_FROM_WRITER;
import static io.pravega.shared.controller.tracing.RPCTracingTags.PING_TRANSACTION;
import static io.pravega.shared.controller.tracing.RPCTracingTags.REMOVE_WRITER;
import static io.pravega.shared.controller.tracing.RPCTracingTags.SCALE_STREAM;
import static io.pravega.shared.controller.tracing.RPCTracingTags.SEAL_STREAM;
import static io.pravega.shared.controller.tracing.RPCTracingTags.TRUNCATE_STREAM;
import static io.pravega.shared.controller.tracing.RPCTracingTags.UPDATE_READER_GROUP;
import static io.pravega.shared.controller.tracing.RPCTracingTags.UPDATE_STREAM;
import static io.pravega.shared.controller.tracing.RPCTracingTags.UPDATE_TRUNCATION_STREAM_CUT;
import static io.pravega.shared.controller.tracing.RPCTracingTags.LIST_COMPLETED_TRANSACTIONS;
import static io.pravega.shared.controller.tracing.RPCTracingTags.GET_CONTROLLER_TO_BUCKET_MAPPING;

/**
 * gRPC Service API implementation for the Controller.
 */
@AllArgsConstructor
public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ControllerServiceImpl.class));
    private static final int PAGE_LIMIT = 1000;

    // The underlying Controller Service implementation to delegate all API calls to.
    private final ControllerService controllerService;

    private final GrpcAuthHelper grpcAuthHelper;

    private final RequestTracker requestTracker;

    // Send to the client server traces on error message replies.
    private final boolean replyWithStackTraceOnError;

    private final boolean isRGStreamWritesWithReadPermEnabled;

    private final int pageLimit;

    private final AuthorizationResource authorizationResource = new AuthorizationResourceImpl();

    public ControllerServiceImpl(ControllerService controllerService, GrpcAuthHelper authHelper,
                                 RequestTracker requestTracker, boolean replyWithStackTraceOnError,
                                 boolean isRGStreamWritesWithReadPermEnabled) {
        this(controllerService, authHelper, requestTracker, replyWithStackTraceOnError,
                isRGStreamWritesWithReadPermEnabled, PAGE_LIMIT);
    }

    @Override
    public void getControllerServerList(ServerRequest request, StreamObserver<ServerResponse> responseObserver) {
        log.trace("getControllerServerList called.");
        authenticateExecuteAndProcessResults(() -> "",
                delegationToken -> controllerService.getControllerServerList()
                                     .thenApply(servers -> ServerResponse.newBuilder().addAllNodeURI(servers).build()),
                responseObserver);
    }

    @Override
    public void createReaderGroup(ReaderGroupConfiguration request, StreamObserver<CreateReaderGroupResponse> responseObserver) {
        String scope = request.getScope();
        String rgName = request.getReaderGroupName();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CREATE_READER_GROUP, scope, rgName);
        log.info(requestTag.getRequestId(), "createReaderGroup called for ReaderGroup {}/{}.", scope, rgName);
        AuthHandler.Permissions requiredPermission = this.isRGStreamWritesWithReadPermEnabled ?
                AuthHandler.Permissions.READ : AuthHandler.Permissions.READ_UPDATE;

        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofReaderGroupsInScope(scope), requiredPermission),
                delegationToken -> controllerService.createReaderGroup(scope, rgName,
                        ModelHelper.encode(request), System.currentTimeMillis(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void updateReaderGroup(ReaderGroupConfiguration request, StreamObserver<UpdateReaderGroupResponse> responseObserver) {
        String scope = request.getScope();
        String rgName = request.getReaderGroupName();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                UPDATE_READER_GROUP, scope, rgName);
        log.info(requestTag.getRequestId(), "updateReaderGroup called for ReaderGroup {}/{}.", scope, rgName);
        AuthHandler.Permissions requiredPermission = this.isRGStreamWritesWithReadPermEnabled ?
                AuthHandler.Permissions.READ : AuthHandler.Permissions.READ_UPDATE;

        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofReaderGroupsInScope(scope), requiredPermission),
                delegationToken -> controllerService.updateReaderGroup(scope, rgName, ModelHelper.encode(request),
                        requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void getReaderGroupConfig(ReaderGroupInfo request, StreamObserver<ReaderGroupConfigResponse> responseObserver) {
        String scope = request.getScope();
        String rgName = request.getReaderGroup();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_READER_GROUP_CONFIG, scope, rgName);
        log.info(requestTag.getRequestId(), "getReaderGroupConfig called for Reader Group {}/{}.", scope, rgName);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofReaderGroupsInScope(scope), AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getReaderGroupConfig(scope, rgName, requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void deleteReaderGroup(ReaderGroupInfo request, StreamObserver<DeleteReaderGroupStatus> responseObserver) {
        String scope = request.getScope();
        String rgName = request.getReaderGroup();
        String rgId = request.getReaderGroupId();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                DELETE_READER_GROUP, scope, rgName);
        log.info(requestTag.getRequestId(), "deleteReaderGroup called for Reader Group {}/{}.", scope, rgName);
        AuthHandler.Permissions requiredPermission = this.isRGStreamWritesWithReadPermEnabled ?
                AuthHandler.Permissions.READ : AuthHandler.Permissions.READ_UPDATE;

        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofReaderGroupsInScope(scope), requiredPermission),
                delegationToken -> controllerService.deleteReaderGroup(scope, rgName, rgId, requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void createKeyValueTable(KeyValueTableConfig request, StreamObserver<CreateKeyValueTableStatus> responseObserver) {
        String scope = request.getScope();
        String kvt = request.getKvtName();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CREATE_KEY_VALUE_TABLE, scope, kvt);
        log.info(requestTag.getRequestId(), "createKeyValueTable called for KVTable {}/{}.", scope, kvt);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofKeyValueTablesInScope(scope), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createKeyValueTable(scope, kvt,
                        ModelHelper.encode(request),
                        System.currentTimeMillis(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void getCurrentSegmentsKeyValueTable(KeyValueTableInfo request, StreamObserver<SegmentRanges> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_CURRENT_SEGMENTS_KEY_VALUE_TABLE, request.getScope(), request.getKvtName());

        log.info(requestTag.getRequestId(), 
                "getCurrentSegmentsKeyValueTable called for kvtable {}/{}.", request.getScope(), request.getKvtName());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofKeyValueTableInScope(request.getScope(), request.getKvtName()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> {
                    logIfEmpty(delegationToken, "getCurrentSegmentsKeyValueTable", request.getScope(),
                            request.getKvtName());
                    return controllerService.getCurrentSegmentsKeyValueTable(request.getScope(), request.getKvtName(),
                            requestTag.getRequestId())
                            .thenApply(segmentRanges -> SegmentRanges.newBuilder()
                                    .addAllSegmentRanges(segmentRanges)
                                    .setDelegationToken(delegationToken)
                                    .build());
                },
                responseObserver);
    }

    @Override
    public void listKeyValueTablesInScope(Controller.KVTablesInScopeRequest request, 
                                          StreamObserver<Controller.KVTablesInScopeResponse> responseObserver) {
        String scopeName = request.getScope().getScope();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                LIST_KEY_VALUE_TABLES, scopeName);
        log.info(requestTag.getRequestId(), "listKeyValueTables called for scope {}.", scopeName);

        final AuthContext ctx = this.grpcAuthHelper.isAuthEnabled() ? AuthContext.current() : null;
        
        Function<String, CompletableFuture<Controller.KVTablesInScopeResponse>> streamsFn = delegationToken ->
                listWithFilter(request.getContinuationToken().getToken(), pageLimit, 
                        (x, y) -> controllerService.listKeyValueTables(scopeName, x, y, requestTag.getRequestId()), 
                        x -> grpcAuthHelper.isAuthorized(authorizationResource.ofKeyValueTableInScope(scopeName, x), 
                                AuthHandler.Permissions.READ, ctx),
                        x -> KeyValueTableInfo.newBuilder().setScope(scopeName).setKvtName(x).build(), requestTag.getRequestId())
                        .handle((response, ex) -> {
                            if (ex != null) {
                                if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                    return Controller.KVTablesInScopeResponse
                                            .newBuilder().setStatus(Controller.KVTablesInScopeResponse.Status.SCOPE_NOT_FOUND)
                                            .build();
                                } else {
                                    throw new CompletionException(ex);
                                }
                            } else {
                                return Controller.KVTablesInScopeResponse
                                        .newBuilder().addAllKvtables(response.getKey())
                                        .setContinuationToken(Controller.ContinuationToken
                                                .newBuilder().setToken(response.getValue()).build())
                                        .setStatus(Controller.KVTablesInScopeResponse.Status.SUCCESS).build();
                            }
                        });

        authenticateExecuteAndProcessResults(
                () -> {
                    String result = this.grpcAuthHelper.checkAuthorization(
                            authorizationResource.ofScope(scopeName),
                            AuthHandler.Permissions.READ,
                            ctx);
                    log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                            authorizationResource.ofScope(scopeName), result);
                    return result;
                }, streamsFn, responseObserver, requestTag);

    }

    @Override
    public void getKeyValueTableConfiguration(KeyValueTableInfo request, StreamObserver<Controller.KeyValueTableConfigResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_KEY_VALUE_TABLE_CONFIGURATION, request.getScope(), request.getKvtName());

        log.info(requestTag.getRequestId(), "{} called for {}/{}.", GET_KEY_VALUE_TABLE_CONFIGURATION,
                request.getScope(), request.getKvtName());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofKeyValueTableInScope(request.getScope(), request.getKvtName()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getKeyValueTableConfiguration(request.getScope(), request.getKvtName(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void deleteKeyValueTable(KeyValueTableInfo request, StreamObserver<DeleteKVTableStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                DELETE_KEY_VALUE_TABLE, request.getScope(), request.getKvtName());

        log.info(requestTag.getRequestId(), "deleteKeyValueTable called for KVTable {}/{}.",
                request.getScope(), request.getKvtName());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofKeyValueTableInScope(request.getScope(), request.getKvtName()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.deleteKeyValueTable(request.getScope(), request.getKvtName(), requestTag.getRequestId()),
                                                        responseObserver, requestTag);
    }

    @Override
    public void createStream(StreamConfig request, StreamObserver<CreateStreamStatus> responseObserver) {
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CREATE_STREAM, scope, stream);
        log.info(requestTag.getRequestId(), "createStream called for stream {}/{}.", scope, stream);
        StreamAuthParams streamAuthParams = new StreamAuthParams(scope, stream, this.isRGStreamWritesWithReadPermEnabled);
        AuthHandler.Permissions requiredPermission = streamAuthParams.requiredPermissionForWrites();

        log.debug(requestTag.getRequestId(), "Creating stream : requiredPermission is [{}], for scope [{}] and stream [{}]",
                requiredPermission, scope, stream);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofStreamsInScope(scope), requiredPermission),
                delegationToken -> controllerService.createStream(scope, stream,
                        ModelHelper.encode(request),
                        System.currentTimeMillis(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void listSubscribers(StreamInfo request, StreamObserver<SubscribersResponse> responseObserver) {
        String scope = request.getScope();
        String stream = request.getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                LIST_SUBSCRIBERS, scope, stream);
        log.info(requestTag.getRequestId(), "listSubscribers called for stream {}/{}.", scope, stream);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(scope, stream), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.listSubscribers(scope, stream, requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void updateSubscriberStreamCut(SubscriberStreamCut request, StreamObserver<UpdateSubscriberStatus> responseObserver) {
        String scope = request.getStreamCut().getStreamInfo().getScope();
        String stream = request.getStreamCut().getStreamInfo().getStream();
        String subscriber = request.getSubscriber();
        String readerGroupId = request.getReaderGroupId();
        long generation = request.getGeneration();
        StreamCut streamCut = request.getStreamCut();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                UPDATE_TRUNCATION_STREAM_CUT, scope, stream);
        log.info(requestTag.getRequestId(), "updateSubscriberStreamCut called for stream {}/{}.", scope, stream);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(scope, stream), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.updateSubscriberStreamCut(scope, stream, subscriber, readerGroupId, 
                        generation, ImmutableMap.copyOf(ModelHelper.encode(streamCut)), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void updateStream(StreamConfig request, StreamObserver<UpdateStreamStatus> responseObserver) {
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                UPDATE_STREAM, scope, stream);
        log.info(requestTag.getRequestId(), "updateStream called for stream {}/{}.", scope, stream);

        Supplier<String> authorizationSupplier = () -> this.grpcAuthHelper.checkAuthorization(
                StreamAuthParams.toResourceString(scope, stream), AuthHandler.Permissions.READ_UPDATE);

        authenticateExecuteAndProcessResults(authorizationSupplier,
                authorizationResult -> controllerService.updateStream(scope, stream, ModelHelper.encode(request), 
                        requestTag.getRequestId()), responseObserver, requestTag);
    }

    @Override
    public void truncateStream(Controller.StreamCut request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                TRUNCATE_STREAM, request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info(requestTag.getRequestId(), "truncateStream called for stream {}/{}.",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.truncateStream(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(), ModelHelper.encode(request), requestTag.getRequestId()), 
                responseObserver, requestTag);
    }

    @Override
    public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                SEAL_STREAM, request.getScope(), request.getStream());

        log.info(requestTag.getRequestId(), "sealStream called for stream {}/{}.",
                request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getScope(), request.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.sealStream(request.getScope(), request.getStream(), 
                        requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void deleteStream(StreamInfo request, StreamObserver<DeleteStreamStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                DELETE_STREAM, request.getScope(), request.getStream());

        log.info(requestTag.getRequestId(), "deleteStream called for stream {}/{}.",
                request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getScope(), request.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.deleteStream(request.getScope(), request.getStream(), 
                        requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    private AccessOperation translate(@NonNull StreamInfo.AccessOperation accessOperation) {
        return AccessOperation.valueOf(accessOperation.name());
    }

    @Override
    public void getCurrentSegments(StreamInfo request, StreamObserver<SegmentRanges> responseObserver) {
        final String scope = request.getScope();
        final String stream = request.getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_CURRENT_SEGMENTS, request.getScope(), request.getStream());

        log.info(requestTag.getRequestId(), "getCurrentSegments called for stream {}/{}.", scope, stream);
        String resource = StreamAuthParams.toResourceString(scope, stream);

        final boolean isDelegationTokenRequested = !request.getAccessOperation().equals(StreamInfo.AccessOperation.NONE);

        authenticateExecuteAndProcessResults(() -> {
                    if (isDelegationTokenRequested) {
                        // For backward compatibility: older clients still depend on delegation tokens generated
                        // by this method for both reads and writes.
                        return this.grpcAuthHelper.checkAuthorizationAndCreateToken(resource, AuthHandler.Permissions.READ_UPDATE);
                    } else {
                        return this.grpcAuthHelper.checkAuthorization(resource, AuthHandler.Permissions.READ);
                    }
                },
                authorizationResult -> {
                    logIfEmpty(authorizationResult, "getCurrentSegments", scope, stream);
                    return controllerService.getCurrentSegments(scope, stream, requestTag.getRequestId())
                            .thenApply(segmentRanges -> {
                                SegmentRanges.Builder builder = SegmentRanges.newBuilder().addAllSegmentRanges(segmentRanges);
                                if (isDelegationTokenRequested) {
                                    builder.setDelegationToken(authorizationResult);
                                }
                                return builder.build();
                            });
                },
                responseObserver, requestTag);
    }

    @VisibleForTesting
    public Supplier<String> delegationTokenSupplier(StreamInfo request) {
        return () -> {
            if (!this.isAuthEnabled()) {
                return "";
            }

            StreamAuthParams authParams = new StreamAuthParams(request.getScope(), request.getStream(),
                    translate(request.getAccessOperation()), this.isRGStreamWritesWithReadPermEnabled);

            // StreamResource will be a stream representation (ex: "prn:://scope:myScope/stream:_RGmyApp") of the
            // reader group (ex: "prn:://scope:myScope/reader-group:myApp). We use stream representation in claims
            // put in delegation tokens for Segment Store's use, even though we use the regular representation for
            // authorization here in the Controller.
            String streamResource = authParams.streamResourceString();
            String resource = authParams.resourceString();

            if (authParams.isAccessOperationUnspecified()) {
                // For backward compatibility: Older clients will not send access operation in the request.
                log.debug("Access operation was unspecified for request with scope {} and stream {}",
                        request.getScope(), request.getStream());
                final AuthHandler.Permissions authAndTokenPermission = AuthHandler.Permissions.READ_UPDATE;
                this.grpcAuthHelper.checkAuthorization(resource, authAndTokenPermission);
                return this.grpcAuthHelper.createDelegationToken(streamResource, authAndTokenPermission);
            } else {
                log.trace("Access operation was {} for request with scope {} and stream {}",
                        translate(request.getAccessOperation()), request.getScope(), request.getStream());

                // The resource string that'll be used in the delegation token for use of the segment store
                final String tokenResource;

                // The operation that'll be specified as granted for the resource in the token. The bearer of the token
                // will be allowed to perform the specified operation.
                final AuthHandler.Permissions tokenPermission;

                // This is the permission that the client is requesting to be assigned on the delegation token.
                AuthHandler.Permissions requestedPermissions = authParams.requestedPermission();

                if (authParams.isStreamUserDefined()) {
                    // The operation itself requires the caller to possess read permissions.
                    AuthHandler.Permissions minimumPermissions = AuthHandler.Permissions.READ;

                    if (requestedPermissions.equals(AuthHandler.Permissions.READ_UPDATE) ||
                            requestedPermissions.equals(minimumPermissions)) {
                        this.grpcAuthHelper.checkAuthorization(streamResource, requestedPermissions);
                        tokenResource = streamResource;
                        tokenPermission = requestedPermissions;
                    } else {
                        // The minimum permission that the user must have to be able to invoke this call. This
                        // authorizes the operation.
                        this.grpcAuthHelper.checkAuthorization(streamResource, minimumPermissions);

                        // Here, we check whether the user is authorized for the requested access.
                        this.grpcAuthHelper.checkAuthorization(streamResource, requestedPermissions);
                        tokenResource = streamResource;
                        tokenPermission = requestedPermissions;
                    }
                } else {
                    final AuthHandler.Permissions authorizationPermission;
                    if (requestedPermissions.equals(AuthHandler.Permissions.READ_UPDATE)) {
                        authorizationPermission = authParams.requiredPermissionForWrites();
                        tokenPermission = AuthHandler.Permissions.READ_UPDATE;
                    } else if (requestedPermissions.equals(AuthHandler.Permissions.READ)) {
                        authorizationPermission = AuthHandler.Permissions.READ;
                        tokenPermission = requestedPermissions;
                    } else {
                        authorizationPermission = AuthHandler.Permissions.READ;
                        tokenPermission = AuthHandler.Permissions.READ;
                    }
                    log.trace("resource: {}, authorizationPermission: {}", authParams.resourceString(), 
                            authorizationPermission);
                    this.grpcAuthHelper.checkAuthorization(authParams.resourceString(), authorizationPermission);
                    tokenResource = streamResource;
                }
                return this.grpcAuthHelper.createDelegationToken(tokenResource, tokenPermission);
            }
        };
    }

    @Override
    public void getEpochSegments(GetEpochSegmentsRequest request, StreamObserver<SegmentRanges> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_EPOCH_SEGMENTS, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                Integer.toString(request.getEpoch()));
        log.info(requestTag.getRequestId(), "getEpochSegments called for stream {}/{} and epoch {}", 
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), request.getEpoch());

        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.
                        Permissions.READ_UPDATE),
                delegationToken -> {
                    logIfEmpty(delegationToken, "getEpochSegments", request.getStreamInfo().getScope(), 
                            request.getStreamInfo().getStream());
                    return controllerService.getEpochSegments(request.getStreamInfo().getScope(), 
                            request.getStreamInfo().getStream(), request.getEpoch(), requestTag.getRequestId())
                                            .thenApply(segmentRanges -> SegmentRanges.newBuilder()
                                                                                     .addAllSegmentRanges(segmentRanges)
                                                                                     .setDelegationToken(delegationToken)
                                                                                     .build());
                },
                responseObserver, requestTag);
    }

    @Override
    public void getSegments(GetSegmentsRequest request, StreamObserver<SegmentsAtTime> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_SEGMENTS, request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        final StreamInfo streamInfo = request.getStreamInfo();
        final String scope = streamInfo.getScope();
        final String stream = streamInfo.getStream();
        log.debug(requestTag.getRequestId(), "getSegments called for stream " + scope + "/" + stream);

        // Older clients will not set requestPermissions, so it'll be set as "". Newer clients will set it as `NONE`.
        // For backward compatibility, this operation returns a delegation token for older clients along with the
        // segments. For newer clients, it doesn't.
        final boolean shouldReturnDelegationToken = !streamInfo.getAccessOperation().equals(StreamInfo.AccessOperation.NONE);
        authenticateExecuteAndProcessResults(() -> {
                    if (shouldReturnDelegationToken) {
                        // For backward compatibility: older clients still depend on delegation token generated
                        // by this method for both reads and writes.
                        return this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                                authorizationResource.ofStreamInScope(scope, stream),
                                AuthHandler.Permissions.READ_UPDATE);
                    } else {
                        return this.grpcAuthHelper.checkAuthorization(
                                authorizationResource.ofStreamInScope(scope, stream),
                                AuthHandler.Permissions.READ);
                    }
                },
                authorizationResult -> {
                    logIfEmpty(authorizationResult, "getSegments", scope, stream);
                    return controllerService.getSegmentsAtHead(scope, stream, requestTag.getRequestId())
                            .thenApply(segments -> {
                                SegmentsAtTime.Builder builder = SegmentsAtTime.newBuilder();
                                if (shouldReturnDelegationToken) {
                                        builder.setDelegationToken(authorizationResult);
                                }
                                for (Entry<SegmentId, Long> entry : segments.entrySet()) {
                                    builder.addSegments(SegmentLocation.newBuilder()
                                            .setSegmentId(entry.getKey())
                                            .setOffset(entry.getValue())
                                            .build());
                                }
                                return builder.build();
                            });
                },
                responseObserver);
    }

    @Override
    public void getSegmentsImmediatelyFollowing(SegmentId segmentId, StreamObserver<SuccessorResponse> responseObserver) {
        String segment = NameUtils.getQualifiedStreamSegmentName(segmentId.getStreamInfo().getScope(),
                segmentId.getStreamInfo().getStream(), segmentId.getSegmentId());
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_SEGMENTS_IMMEDIATELY_FOLLOWING, segment);

        log.info(requestTag.getRequestId(), "getSegmentsImmediatelyFollowing called for segment {} ", segment);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(segmentId.getStreamInfo().getScope(),
                        segmentId.getStreamInfo().getStream()), AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getSegmentsImmediatelyFollowing(segmentId, requestTag.getRequestId())
                                       .thenApply(ModelHelper::createSuccessorResponse)
                                       .thenApply(response -> {
                                           response.setDelegationToken(delegationToken);
                                           return response.build();
                                       }),
                responseObserver);
    }

    /* This deprecated call should be removed once we address: https://github.com/pravega/pravega/issues/3760 */
    @Override
    public void getSegmentsImmediatlyFollowing(SegmentId segmentId, StreamObserver<SuccessorResponse> responseObserver) {
        getSegmentsImmediatelyFollowing(segmentId, responseObserver);
    }

    @Override
    public void getSegmentsBetween(Controller.StreamCutRange request, 
                                   StreamObserver<Controller.StreamCutRangeResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_SEGMENTS_BETWEEN_STREAM_CUTS, request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info(requestTag.getRequestId(), "getSegmentsBetweenStreamCuts called for stream {} for cuts from {} to {}", 
                request.getStreamInfo(), request.getFromMap(), request.getToMap());
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofStreamInScope(scope, stream), AuthHandler.Permissions.READ),
                delegationToken -> {
                    logIfEmpty(delegationToken, "getSegmentsBetween", request.getStreamInfo().getScope(),
                            request.getStreamInfo().getStream());
                    return controllerService.getSegmentsBetweenStreamCuts(request, requestTag.getRequestId())
                            .thenApply(segments -> ModelHelper.createStreamCutRangeResponse(scope, stream,
                                    segments.stream().map(x -> ModelHelper.createSegmentId(scope, stream, x.segmentId()))
                                            .collect(Collectors.toList()), delegationToken));
                },
                responseObserver);
    }

    @Override
    public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                SCALE_STREAM, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), 
                String.valueOf(request.getScaleTimestamp()));

        log.info(requestTag.getRequestId(), "scale called for stream {}/{}.",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.scale(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getSealedSegmentsList(),
                        request.getNewKeyRangesList().stream().collect(Collectors.toMap(
                                ScaleRequest.KeyRangeEntry::getStart, ScaleRequest.KeyRangeEntry::getEnd)),
                        request.getScaleTimestamp(), requestTag.getRequestId()),
                responseObserver);
    }

    @Override
    public void checkScale(ScaleStatusRequest request, StreamObserver<ScaleStatusResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CHECK_SCALE, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), Integer.toString(request.getEpoch()));

        log.debug(requestTag.getRequestId(), "check scale status called for stream {}/{}.", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.checkScale(request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                        request.getEpoch(), requestTag.getRequestId()), responseObserver);
    }

    @Override
    public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
        String segment = NameUtils.getQualifiedStreamSegmentName(request.getStreamInfo().getScope(), 
                request.getStreamInfo().getStream(), request.getSegmentId());
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_URI, segment);
        log.info(requestTag.getRequestId(), "getURI called for segment {}.", segment);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                StreamAuthParams.toResourceString(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getURI(request),
                responseObserver);
    }

    @Override
    public void isSegmentValid(SegmentId request,
                               StreamObserver<SegmentValidityResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                IS_SEGMENT_OPEN, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), Long.toString(request.getSegmentId()));

        log.info(requestTag.getRequestId(), "isSegmentValid called for segment {}/{}/{}.", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.isSegmentValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getSegmentId(), requestTag.getRequestId())
                                       .thenApply(bRes -> SegmentValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void isStreamCutValid(Controller.StreamCut request, 
                                 StreamObserver<Controller.StreamCutValidityResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                IS_STREAMCUT_VALID, request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info(requestTag.getRequestId(), "isStreamCutValid called for stream {}/{} streamcut {}.", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getCutMap());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.isStreamCutValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getCutMap(), requestTag.getRequestId())
                        .thenApply(bRes -> Controller.StreamCutValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CREATE_TRANSACTION, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                Long.toString(request.getLease()));
        log.info(requestTag.getRequestId(), "createTransaction called for stream {}/{}.",
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());

        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorizationAndCreateToken(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getLease(), requestTag.getRequestId())
                                       .thenApply(pair -> Controller.CreateTxnResponse.newBuilder()
                                                                                      .setDelegationToken(delegationToken)
                                                                                      .setTxnId(decode(pair.getKey()))
                                                                                      .addAllActiveSegments(pair.getValue())
                                                                                      .build()),
                responseObserver, requestTag);
    }

    @Override
    public void commitTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        final UUID txnId = ModelHelper.encode(request.getTxnId());
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                COMMIT_TRANSACTION, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), txnId.toString());

        log.info(requestTag.getRequestId(), "commitTransaction called for stream {}/{}, txnId={}.", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), txnId);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(
                        request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.commitTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        txnId, request.getWriterId(), request.getTimestamp(), requestTag.getRequestId()),
                responseObserver);
    }

    @Override
    public void abortTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        final UUID txnId = ModelHelper.encode(request.getTxnId());
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                ABORT_TRANSACTION, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), txnId.toString());

        log.info(requestTag.getRequestId(), "abortTransaction called for stream {}/{}, txnId={}.", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), txnId);
        authenticateExecuteAndProcessResults( () -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.abortTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        txnId, requestTag.getRequestId()),
                responseObserver);
    }

    @Override
    public void pingTransaction(PingTxnRequest request, StreamObserver<PingTxnStatus> responseObserver) {
        final UUID txnId = ModelHelper.encode(request.getTxnId());
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                PING_TRANSACTION, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), txnId.toString());
        log.info(requestTag.getRequestId(), "pingTransaction called for stream {}/{}, txnId={}", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), txnId);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
               delegationToken  -> controllerService.pingTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        txnId,
                        request.getLease(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void checkTransactionState(TxnRequest request, StreamObserver<TxnState> responseObserver) {
        final UUID txnId = ModelHelper.encode(request.getTxnId());
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CHECK_TRANSACTION_STATE, request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), 
                txnId.toString());
        log.info(requestTag.getRequestId(), "checkTransactionState called for stream {}/{}, txnId={}.", 
                request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), txnId);
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.checkTransactionStatus(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        txnId, requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void listCompletedTransactions(Controller.ListCompletedTxnRequest request, StreamObserver<Controller.ListCompletedTxnResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(), LIST_COMPLETED_TRANSACTIONS,
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        log.info(requestTag.getRequestId(), "listCompletedTransactions called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                        authorizationResource.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                        AuthHandler.Permissions.READ),
                delegationToken -> controllerService.listCompletedTxns(request.getStreamInfo().getScope(),
                                request.getStreamInfo().getStream(), requestTag.getRequestId())
                        .thenApply(result -> Controller.ListCompletedTxnResponse.newBuilder()
                                .addAllResponse(result.entrySet().stream().map(x -> Controller.TxnResponse.newBuilder()
                                                .setTxnId(decode(x.getKey())).setStatus(Controller.TxnResponse.Status.valueOf(x.getValue().name())).build())
                                        .collect(Collectors.toList()))
                                .build()),
                responseObserver, requestTag);
    }

    @Override
    public void createScope(ScopeInfo request, StreamObserver<CreateScopeStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CREATE_SCOPE, request.getScope());
        log.info(requestTag.getRequestId(), "createScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofScopes(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createScope(request.getScope(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void listScopes(Controller.ScopesRequest request, StreamObserver<Controller.ScopesResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                LIST_SCOPES);
        log.info(requestTag.getRequestId(), "listScope called.");

        final AuthContext ctx;
        if (this.grpcAuthHelper.isAuthEnabled()) {
            ctx = AuthContext.current();
        } else {
            ctx = null;
        }

        Supplier<String> stringSupplier = () -> {
            String result = this.grpcAuthHelper.checkAuthorization(
                    authorizationResource.ofScopes(),
                    AuthHandler.Permissions.READ,
                    ctx);
            log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                    authorizationResource.ofScopes(), result);
            return result;
        };
        
        Function<String, CompletableFuture<Controller.ScopesResponse>> scopesFn = delegationToken ->
                listWithFilter(request.getContinuationToken().getToken(), pageLimit,
                        (x, y) -> controllerService.listScopes(x, y, requestTag.getRequestId()),
                        x -> grpcAuthHelper.isAuthorized(authorizationResource.ofScope(x), AuthHandler.Permissions.READ, ctx),
                        x -> x, requestTag.getRequestId())
                        .thenApply(response -> Controller.ScopesResponse
                                .newBuilder().addAllScopes(response.getKey())
                                .setContinuationToken(Controller.ContinuationToken.newBuilder()
                                                                                  .setToken(response.getValue()).build())
                                .build());

        authenticateExecuteAndProcessResults(stringSupplier, scopesFn, responseObserver, requestTag);
    }
    
    @Override
    public void checkScopeExists(ScopeInfo request, StreamObserver<Controller.ExistsResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CHECK_SCOPE_EXISTS, request.getScope());
        String scope = request.getScope();
        log.info(requestTag.getRequestId(), "checkScopeExists called for scope {}.", request);

        final AuthContext ctx;
        if (this.grpcAuthHelper.isAuthEnabled()) {
            ctx = AuthContext.current();
        } else {
            ctx = null;
        }

        Supplier<String> stringSupplier = () -> {
            String result = this.grpcAuthHelper.checkAuthorization(
                    authorizationResource.ofScope(scope),
                    AuthHandler.Permissions.READ,
                    ctx);
            log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                    authorizationResource.ofScopes(), result);
            return result;
        };
        Function<String, CompletableFuture<Controller.ExistsResponse>> scopeFn = delegationToken -> controllerService
                .getScope(scope, requestTag.getRequestId())
                .handle((response, e) -> {
                    boolean exists;
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            exists = false;
                        } else {
                            throw new CompletionException(e);
                        }
                    } else {
                        exists = true;
                    }
                    return Controller.ExistsResponse.newBuilder().setExists(exists).build();
                });
        authenticateExecuteAndProcessResults(stringSupplier, scopeFn, responseObserver, requestTag);
    }

    @Override
    public void checkStreamExists(StreamInfo request, StreamObserver<Controller.ExistsResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                CHECK_STREAM_EXISTS);
        String scope = request.getScope();
        String stream = request.getStream();
        log.info(requestTag.getRequestId(), "checkStream exists called for {}/{}.", scope, stream);

        final AuthContext ctx;
        if (this.grpcAuthHelper.isAuthEnabled()) {
            ctx = AuthContext.current();
        } else {
            ctx = null;
        }

        Supplier<String> stringSupplier = () -> {
            String result = this.grpcAuthHelper.checkAuthorization(
                    authorizationResource.ofStreamInScope(scope, stream),
                    AuthHandler.Permissions.READ,
                    ctx);
            log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                    authorizationResource.ofScopes(), result);
            return result;
        };
        Function<String, CompletableFuture<Controller.ExistsResponse>> streamFn = delegationToken -> controllerService
                .getStream(scope, stream, requestTag.getRequestId())
                .handle((response, e) -> {
                    boolean exists;
                    if (e != null) {
                        if (Exceptions.unwrap(e) instanceof StoreException.DataNotFoundException) {
                            exists = false;
                        } else {
                            throw new CompletionException(e);
                        }
                    } else {
                        exists = true;
                    }
                    return Controller.ExistsResponse
                            .newBuilder().setExists(exists)
                            .build();
                });
        authenticateExecuteAndProcessResults(stringSupplier, streamFn, responseObserver, requestTag);
    }

    @Override
    public void getStreamConfiguration(StreamInfo request, StreamObserver<StreamConfig> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                                                                            GET_STREAM_CONFIGURATION);
        String scope = request.getScope();
        String stream = request.getStream();
        log.info(requestTag.getRequestId(), "{} called for {}/{}.", GET_STREAM_CONFIGURATION, scope, stream);

        final AuthContext ctx;
        if (this.grpcAuthHelper.isAuthEnabled()) {
            ctx = AuthContext.current();
        } else {
            ctx = null;
        }

        Supplier<String> stringSupplier = () -> {
            String result = this.grpcAuthHelper.checkAuthorization(
                    authorizationResource.ofStreamInScope(scope, stream),
                    AuthHandler.Permissions.READ,
                    ctx);
            log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                      authorizationResource.ofScopes(), result);
            return result;
        };
        Function<String, CompletableFuture<StreamConfig>> streamFn = delegationToken -> controllerService
                .getStream(scope, stream, requestTag.getRequestId())
                .handle((response, e) -> {
                    if (e != null) {
                        throw new CompletionException(e);
                    } else {
                        return decode(scope, stream, response);

                    }
                });
        authenticateExecuteAndProcessResults(stringSupplier, streamFn, responseObserver, requestTag);
    }

    @Override
    public void listStreamsInScope(Controller.StreamsInScopeRequest request, 
                                   StreamObserver<Controller.StreamsInScopeResponse> responseObserver) {
        String scopeName = request.getScope().getScope();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                LIST_STREAMS_IN_SCOPE, scopeName);
        log.info(requestTag.getRequestId(), "listStream called for scope {}.", scopeName);

        final AuthContext ctx = this.grpcAuthHelper.isAuthEnabled() ? AuthContext.current() : null;
        Function<String, CompletableFuture<Controller.StreamsInScopeResponse>> streamsFn = delegationToken ->
                listWithFilter(request.getContinuationToken().getToken(), pageLimit,
                        (x, y) -> controllerService.listStreams(scopeName, x, y, requestTag.getRequestId()),
                        x -> grpcAuthHelper.isAuthorized(authorizationResource.ofStreamInScope(scopeName, x), 
                                AuthHandler.Permissions.READ, ctx),
                        x -> StreamInfo.newBuilder().setScope(scopeName).setStream(x).build(), requestTag.getRequestId())
                        .handle((response, ex) -> {
                            if (ex != null) {
                                if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                    return Controller.StreamsInScopeResponse
                                            .newBuilder().setStatus(Controller.StreamsInScopeResponse.Status.SCOPE_NOT_FOUND)
                                            .build();
                                } else {
                                    throw new CompletionException(ex);
                                }
                            } else {
                                return Controller.StreamsInScopeResponse
                                        .newBuilder().addAllStreams(response.getKey())
                                        .setContinuationToken(Controller.ContinuationToken.newBuilder()
                                                                                          .setToken(response.getValue()).build())
                                        .setStatus(Controller.StreamsInScopeResponse.Status.SUCCESS).build();
                            }
                        });

        authenticateExecuteAndProcessResults(
                () -> {
                    String result = this.grpcAuthHelper.checkAuthorization(
                            authorizationResource.ofScope(scopeName),
                            AuthHandler.Permissions.READ,
                            ctx);
                    log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                              authorizationResource.ofScope(scopeName), result);
                    return result;
                }, streamsFn, responseObserver, requestTag);
    }

    @Override
    public void listStreamsInScopeForTag(Controller.StreamsInScopeWithTagRequest request,
                                   StreamObserver<Controller.StreamsInScopeResponse> responseObserver) {
        String scopeName = request.getScope().getScope();
        String tag = request.getTag();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                                                                            LIST_STREAMS_IN_SCOPE_FOR_TAG, scopeName);
        log.info(requestTag.getRequestId(), "{} called for scope {} and tags {}", LIST_STREAMS_IN_SCOPE_FOR_TAG, scopeName, tag);

        final AuthContext ctx = this.grpcAuthHelper.isAuthEnabled() ? AuthContext.current() : null;
        Function<String, CompletableFuture<Controller.StreamsInScopeResponse>> streamsFn = delegationToken ->
                listWithFilter(request.getContinuationToken().getToken(), pageLimit,
                               (x, y) -> controllerService.listStreamsForTag(scopeName, tag, x, requestTag.getRequestId()),
                               x -> grpcAuthHelper.isAuthorized(authorizationResource.ofStreamInScope(scopeName, x),
                                                                AuthHandler.Permissions.READ, ctx),
                               x -> StreamInfo.newBuilder().setScope(scopeName).setStream(x).build(), requestTag.getRequestId())
                        .handle((response, ex) -> {
                            if (ex != null) {
                                if (Exceptions.unwrap(ex) instanceof StoreException.DataNotFoundException) {
                                    return Controller.StreamsInScopeResponse
                                            .newBuilder().setStatus(Controller.StreamsInScopeResponse.Status.SCOPE_NOT_FOUND)
                                            .build();
                                } else {
                                    throw new CompletionException(ex);
                                }
                            } else {
                                return Controller.StreamsInScopeResponse
                                        .newBuilder().addAllStreams(response.getKey())
                                        .setContinuationToken(Controller.ContinuationToken.newBuilder()
                                                                                          .setToken(response.getValue()).build())
                                        .setStatus(Controller.StreamsInScopeResponse.Status.SUCCESS).build();
                            }
                        });

        authenticateExecuteAndProcessResults(
                () -> {
                    String result = this.grpcAuthHelper.checkAuthorization(
                            authorizationResource.ofScope(scopeName),
                            AuthHandler.Permissions.READ,
                            ctx);
                    log.debug("Result of authorization for [{}] and READ permission is: [{}]",
                              authorizationResource.ofScope(scopeName), result);
                    return result;
                }, streamsFn, responseObserver, requestTag);
    }
    
    @Override
    public void deleteScope(ScopeInfo request, StreamObserver<DeleteScopeStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                DELETE_SCOPE, request.getScope());
        log.info(requestTag.getRequestId(), "deleteScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofScopes(), AuthHandler.Permissions.READ_UPDATE),
               delegationToken -> controllerService.deleteScope(request.getScope(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void getDelegationToken(StreamInfo request, StreamObserver<DelegationToken> responseObserver)  {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                GET_OR_REFRESH_DELEGATION_TOKEN_FOR, request.getScope(), request.getStream());
        log.info(requestTag.getRequestId(), 
                "getDelegationToken called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(this.delegationTokenSupplier(request),
                delegationToken -> {
                    logIfEmpty(delegationToken, "getDelegationToken", request.getScope(), request.getStream());
                    return CompletableFuture.completedFuture(DelegationToken
                            .newBuilder()
                            .setDelegationToken(delegationToken)
                            .build());
                },
                responseObserver);
    }
    // region watermarking apis
    
    @Override
    public void noteTimestampFromWriter(Controller.TimestampFromWriter request, 
                                        StreamObserver<Controller.TimestampResponse> responseObserver) {
        StreamInfo streamInfo = request.getPosition().getStreamInfo();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                NOTE_TIMESTAMP_FROM_WRITER, streamInfo.getScope(), streamInfo.getStream(), request.getWriter());
        log.info(requestTag.getRequestId(), "noteWriterMark called for stream {}/{}, writer={} time={}", 
                streamInfo.getScope(),
                streamInfo.getStream(), request.getWriter(), request.getTimestamp());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(streamInfo.getScope(), streamInfo.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken  -> controllerService.noteTimestampFromWriter(streamInfo.getScope(),
                        streamInfo.getStream(), request.getWriter(), request.getTimestamp(), request.getPosition().getCutMap(), 
                        requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void deleteScopeRecursive(ScopeInfo request, StreamObserver<DeleteScopeStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(ControllerService.nextRequestId(),
                DELETE_SCOPE_RECURSIVE, request.getScope());
        log.info(requestTag.getRequestId(), "deleteScopeRecursive called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                        authorizationResource.ofScopes(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.deleteScopeRecursive(request.getScope(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void getControllerToBucketMapping(Controller.ControllerToBucketMappingRequest request,
                                             StreamObserver<Controller.ControllerToBucketMappingResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(ControllerService.nextRequestId(),
                GET_CONTROLLER_TO_BUCKET_MAPPING, request.getServiceType().toString());
        log.info(requestTag.getRequestId(), "getControllerToBucketMapping called for type {}.",
                request.getServiceType().toString());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                        authorizationResource.ofScopes(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.getControllerToBucketMapping(request.getServiceType(),
                        requestTag.getRequestId()).handle((result, ex) -> {
                    if (ex == null) {
                        Controller.ControllerToBucketMappingResponse.Builder controllerToBucketMappingResponse =
                                Controller.ControllerToBucketMappingResponse.newBuilder();
                        result.forEach((k, v) -> controllerToBucketMappingResponse.putMapping(k,
                                Controller.BucketIds.newBuilder().addAllId(new ArrayList<>(v)).build()));
                        return controllerToBucketMappingResponse.build();
                    } else {
                        log.warn("getControllerToBucketMapping call failed due to ", ex);
                        throw new CompletionException(ex);
                    }
                }), responseObserver, requestTag);
    }

    @Override
    public void removeWriter(Controller.RemoveWriterRequest request, 
                             StreamObserver<Controller.RemoveWriterResponse> responseObserver) {
        StreamInfo streamInfo = request.getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(controllerService.nextRequestId(),
                REMOVE_WRITER, streamInfo.getScope(), streamInfo.getStream(), request.getWriter());
        log.info(requestTag.getRequestId(), "writerShutdown called for stream {}/{}, writer={}", streamInfo.getScope(),
                streamInfo.getStream(), request.getWriter());
        authenticateExecuteAndProcessResults(() -> this.grpcAuthHelper.checkAuthorization(
                authorizationResource.ofStreamInScope(streamInfo.getScope(), streamInfo.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken  -> controllerService.removeWriter(streamInfo.getScope(),
                        streamInfo.getStream(), request.getWriter(), requestTag.getRequestId()),
                responseObserver, requestTag);
    }
    // endregion
    
    private void logIfEmpty(String delegationToken, String requestName, String scopeName, String name) {
        if (isAuthEnabled() && Strings.isNullOrEmpty(delegationToken)) {
            log.warn("Delegation token for request [{}] for artifact [{}]/[{}], is: [{}]",
                    requestName, scopeName, name, delegationToken);
        }
    }

    // Convert responses from CompletableFuture to gRPC's Observer pattern.
    private <T> void authenticateExecuteAndProcessResults(Supplier<String> authenticator, 
                                                          Function<String, CompletableFuture<T>> call,
                                                          final StreamObserver<T> streamObserver, RequestTag requestTag) {
        try {
            String delegationToken = authenticator.get();
            CompletableFuture<T> result = call.apply(delegationToken);
            result.whenComplete(
                    (value, ex) -> {
                        log.debug("result =  {}", value);
                        if (ex != null) {
                            Throwable cause = Exceptions.unwrap(ex);
                            logError(requestTag, cause);
                            String stackTrace = "controllerStackTrace=" + Throwables.getStackTraceAsString(ex);
                            String errorDescription = replyWithStackTraceOnError ? stackTrace : cause.getMessage();
                            streamObserver.onError(getStatusFromException(cause)
                                    .withCause(cause)
                                    .withDescription(errorDescription)
                                    .asRuntimeException());
                        } else if (value != null) {
                            streamObserver.onNext(value);
                            streamObserver.onCompleted();
                        }
                        logAndUntrackRequestTag(requestTag);
                    });
        } catch (AuthenticationException e) {
            // Empty credentials when Auth is enabled may lead to this exception here. When credentials are present in
            // the client call but are incorrect, the client is returned the same status from AuthInterceptor.
            handleException(e, streamObserver, requestTag, Status.UNAUTHENTICATED, "Authentication failed");
        } catch (AuthorizationException e) {
            handleException(e, streamObserver, requestTag, Status.PERMISSION_DENIED, "Authorization failed");
        } catch (Exception e) {
            handleException(e, streamObserver, requestTag, Status.INTERNAL, "Internal exception occurred");
        }
    }

    private <T> void authenticateExecuteAndProcessResults(Supplier<String> authenticator,
                                                          Function<String, CompletableFuture<T>> call,
                                                          final StreamObserver<T> streamObserver) {
        authenticateExecuteAndProcessResults(authenticator, call, streamObserver, null);
    }

    private void handleException(Exception e, final StreamObserver<?> streamObserver, RequestTag requestTag,
                                 Status status, String message) {
        log.error("Encountered {} in authenticateExecuteAndProcessResults", e.getClass().getSimpleName(), e);
        logAndUntrackRequestTag(requestTag);
        streamObserver.onError(status.withDescription(message).asRuntimeException());
    }
    
    @SuppressWarnings("checkstyle:ReturnCount")
    private Status getStatusFromException(Throwable cause) {
        if (cause instanceof StoreException.DataExistsException) {
            return Status.ALREADY_EXISTS;
        }
        if (cause instanceof StoreException.DataNotFoundException) {
            return Status.NOT_FOUND;
        }
        if (cause instanceof StoreException.DataNotEmptyException) {
            return Status.FAILED_PRECONDITION;
        }
        if (cause instanceof StoreException.WriteConflictException) {
            return Status.FAILED_PRECONDITION;
        }
        if (cause instanceof StoreException.IllegalStateException) {
            return Status.INTERNAL;
        }
        if (cause instanceof StoreException.OperationNotAllowedException) {
            return Status.PERMISSION_DENIED;
        }
        if (cause instanceof StoreException.StoreConnectionException) {
            return Status.INTERNAL;
        }
        if (cause instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED;
        }
        return Status.UNKNOWN;
    }

    private void logAndUntrackRequestTag(RequestTag requestTag) {
        if (requestTag != null) {
            log.debug(requestTracker.untrackRequest(requestTag.getRequestDescriptor()),
                    "Untracking request: {}.", requestTag.getRequestDescriptor());
        }
    }

    private void logError(RequestTag requestTag, Throwable cause) {
        String tag = requestTag == null ? "none" : requestTag.getRequestDescriptor();
        long requestId = requestTag == null ? RequestTag.NON_EXISTENT_ID : requestTag.getRequestId();
        if (cause instanceof LockFailedException) {
            log.warn(requestId, "Controller API call with tag {} failed with: {}", tag, cause.getMessage());
        } else {
            log.error(requestId, "Controller API call with tag {} failed with error: ", tag, cause);
        }
    }

    private <T> CompletableFuture<Pair<List<T>, String>> listWithFilter(
            String continuationToken, int limit, 
            BiFunction<String, Integer, CompletableFuture<Pair<List<String>, String>>> fetcher,
            Predicate<String> filter, Function<String, T> tCreator, long requestId) {
        
        List<T> results = new ArrayList<>();
        return fetcher.apply(continuationToken, limit).thenCompose(response -> {
            log.debug(requestId, "All entries with continuation token: {}", response);
            // filter unauthorized results. 
            // fetch recursively if results are filtered out.
            boolean filtered = false;
            for (String key : response.getKey()) {
                if (filter.test(key)) {
                    results.add(tCreator.apply(key));
                } else {
                    filtered = true;
                }
            }

            if (filtered) {
                return listWithFilter(response.getValue(), limit - results.size(), fetcher, filter, tCreator, requestId)
                              .thenApply(result -> {
                                  results.addAll(result.getKey());
                                  return new ImmutablePair<>(results, result.getValue());
                              });
            } else {
                return CompletableFuture.completedFuture(new ImmutablePair<>(results, response.getValue()));
            }
        });
    }

    private boolean isAuthEnabled() {
        return this.grpcAuthHelper.isAuthEnabled();
    }
}
