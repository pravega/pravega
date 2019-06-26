/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import com.google.common.base.Preconditions;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.pravega.auth.AuthHandler;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.PravegaCredentialsWrapper;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScalingPolicy;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceBlockingStub;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.AssertExtensions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.pravega.controller.auth.AuthFileUtils.credentialsAndAclAsString;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The tests in this class are intended to perform verification of authorization logic in ControllerServiceImpl (the
 * service implementation of the Controller gRPC interface).
 */
public class ControllerGrpcAuthFocusedTest {

    /**
     * These two members are shared across the tests in this class for efficiency reasons.
     */
    private final static ScheduledExecutorService EXECUTOR =
            ExecutorServiceHelpers.newScheduledThreadPool(20, MethodHandles.lookup().lookupClass() + "-pool");
    private final static File AUTH_FILE = createAuthFile();

    private final static String DEFAULT_PASSWORD = "1111_aaaa";

    /**
     * This rule makes sure that the tests in this class run in 10 seconds or less.
     */
    @Rule
    public final Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    /**
     * This rule is used later to expect both the exception class and the message.
     */
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private Server grpcServer;
    private ManagedChannel inProcessChannel;

    @AfterClass
    public static void classTearDown() {
        if (AUTH_FILE != null && AUTH_FILE.exists()) {
            AUTH_FILE.delete();
        }
        ExecutorServiceHelpers.shutdown(EXECUTOR);
    }

    @Before
    public void setup() throws IOException {
        TaskMetadataStore taskMetadataStore = TaskStoreFactory.createInMemoryStore(EXECUTOR);
        HostControllerStore hostStore = HostStoreFactory.createInMemoryStore(HostMonitorConfigImpl.dummyConfig());
        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore(EXECUTOR);
        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        RequestTracker requestTracker = new RequestTracker(true);

        ConnectionFactoryImpl connectionFactory = new ConnectionFactoryImpl(
                ClientConfig.builder()
                        .controllerURI(URI.create("tcp://localhost"))
                        .credentials(new DefaultCredentials(DEFAULT_PASSWORD, UserNames.ADMIN))
                        .build());

        AuthHelper authHelper = new AuthHelper(true, "secret");

        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                EXECUTOR, "host", authHelper, requestTracker);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelper,
                EXECUTOR, "host", authHelper);

        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, streamStore, EXECUTOR),
                new ScaleOperationTask(streamMetadataTasks, streamStore, EXECUTOR),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, EXECUTOR),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, EXECUTOR),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, EXECUTOR),
                new TruncateStreamTask(streamMetadataTasks, streamStore, EXECUTOR),
                streamStore,
                EXECUTOR);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, EXECUTOR));
        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.getClusterMembers()).thenReturn(Collections.singleton(new Host("localhost", 9090, null)));

        ControllerServiceGrpc.ControllerServiceImplBase controllerServiceImplBase = new ControllerServiceImpl(
                new ControllerService(streamStore,
                                      streamMetadataTasks,
                                      streamTransactionMetadataTasks,
                                      segmentHelper,
                                      EXECUTOR,
                                      mockCluster),
                authHelper,
                requestTracker,
                true,
                2);

        AuthHandler authHandler = new PasswordAuthHandler();
        ((PasswordAuthHandler) authHandler).initialize(AUTH_FILE.getAbsolutePath());

        String uniqueServerName = String.format("Test server name: %s", getClass());

        // Using a builder that creates a server for servicing in-process requests.
        // Also, using a direct executor which executes app code directly in transport thread. See
        // https://grpc.io/grpc-java/javadoc/io/grpc/inprocess/InProcessServerBuilder.html for more information.
        grpcServer = InProcessServerBuilder.forName(uniqueServerName)
                .addService(ServerInterceptors.intercept(controllerServiceImplBase,
                        new PravegaInterceptor(authHandler)))
                .directExecutor()
                .build()
                .start();
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
    }

    @After
    public void tearDown() throws Exception {
        if (streamMetadataTasks != null) {
            streamMetadataTasks.close();
        }
        if (streamTransactionMetadataTasks != null) {
            streamTransactionMetadataTasks.close();
        }
        inProcessChannel.shutdownNow();
        grpcServer.shutdownNow();
    }

    @Test
    public void createScopeSucceedsForPrivilegedUser() {
        //Arrange
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        //Act
        CreateScopeStatus status = blockingStub.createScope(Controller.ScopeInfo.newBuilder().setScope("dummy").build());

        //Verify
        assertEquals(CreateScopeStatus.Status.SUCCESS, status.getStatus());
    }

    @Test
    public void createScopeFailsForUnauthorizedUser() {
        //Arrange
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareCallStub(UserNames.SCOPE_READER, DEFAULT_PASSWORD);

        //Verify
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("UNAUTHENTICATED");

        //Act
        blockingStub.createScope(Controller.ScopeInfo.newBuilder().setScope("dummy").build());
    }

    @Test
    public void createScopeFailsForNonExistentUser() {
        //Arrange
        ControllerServiceBlockingStub blockingStub =
                prepareCallStub("whatever", "whatever");

        //Verify
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("UNAUTHENTICATED");

        //Act
        blockingStub.createScope(Controller.ScopeInfo.newBuilder().setScope("dummy").build());
    }

    @Test
    public void getUriSucceedsForPrivilegedUser() {
        String scope = "scope1";
        String stream = "stream1";

        //Arrange
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));

        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        //Act
        NodeUri nodeUri1 = stub.getURI(segmentId(scope, stream, 0));
        NodeUri nodeUri2 = stub.getURI(segmentId(scope, stream, 1));

        //Verify
        assertEquals("localhost", nodeUri1.getEndpoint());
        assertEquals(12345, nodeUri1.getPort());
        assertEquals("localhost", nodeUri2.getEndpoint());
        assertEquals(12345, nodeUri2.getPort());
    }

    @Test
    public void getUriFailsForNonExistentUser() {
        String scope = "scope1";
        String stream = "stream1";

        //Arrange
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareCallStub("nonexistentuser", "whatever");

        //Verify
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("UNAUTHENTICATED");

        //Act
        NodeUri nodeUri1 = stub.getURI(segmentId(scope, stream, 0));
    }

    @Test
    public void isSegmentValidSucceedsForAuthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE1_STREAM1_READ, DEFAULT_PASSWORD);

        assertTrue(stub.isSegmentValid(segmentId(scope, stream, 0)).getResponse());
        assertFalse(stub.isSegmentValid(segmentId(scope, stream, 3)).getResponse());
    }

    @Test
    public void isSegmentValidFailsForUnauthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));

        //Note that the user has READ access to scope1/stream2, not scope1/stream1.
        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE1_STREAM2_READ, DEFAULT_PASSWORD);

        //Set the expected exception
        thrown.expect(StatusRuntimeException.class);
        //thrown.expectMessage();
        thrown.expectMessage("UNAUTHENTICATED");

        stub.isSegmentValid(segmentId(scope, stream, 0));
    }

    @Test
    public void pingTransactionSucceedsForAuthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";

        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        TxnId transactionId = createTransaction(StreamInfo.newBuilder().setScope(scope).setStream(stream).build(), 2000);

        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE1_STREAM1_READUPDATE, DEFAULT_PASSWORD);

        PingTxnStatus status = stub.pingTransaction(Controller.PingTxnRequest.newBuilder()
                    .setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(stream).build())
                    .setTxnId(transactionId)
                    .setLease(1000)
                    .build());
        assertEquals(PingTxnStatus.Status.OK, status.getStatus());
    }

    @Test
    public void pingTransactionFailsForUnAuthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";

        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        TxnId transactionId = createTransaction(StreamInfo.newBuilder().setScope(scope).setStream(stream).build(), 2000);

        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE1_STREAM1_READ, DEFAULT_PASSWORD);

        //Set the expected exception
        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("UNAUTHENTICATED: Authentication failed");

        PingTxnStatus status = stub.pingTransaction(Controller.PingTxnRequest.newBuilder()
                .setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(stream).build())
                .setTxnId(transactionId)
                .setLease(1000)
                .build());
    }


    @Test
    public void listStreamsReturnsAllWhenUserHasWildCardAccess() {
        // Arrange
        String scopeName = "scope1";
        createScopeAndStreams(scopeName, Arrays.asList("stream1", "stream2"),
                prepareFromFixedScaleTypePolicy(2));

        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scopeName).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        // Act
        Controller.StreamsInScopeResponse response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(2, response.getStreamsList().size());
    }

    @Test
    public void listStreamReturnsEmptyResultWhenUserHasNoAccessToStreams() {
        // Arrange
        createScopeAndStreams("scope1", Arrays.asList("stream1", "stream2", "stream3"),
                prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE1_READ, DEFAULT_PASSWORD);
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope("scope1").build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        Controller.StreamsInScopeResponse response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(0, response.getStreamsList().size());
    }

    @Test
    public void listStreamFiltersResultWhenUserHasAccessToSubsetOfStreams() {
        // Arrange
        createScopeAndStreams("scope1", Arrays.asList("stream1", "stream2", "stream3"),
                prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE1_STREAM1_LIST_READ, DEFAULT_PASSWORD);
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope("scope1").build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        Controller.StreamsInScopeResponse response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(1, response.getStreamsList().size());
    }

    @Test
    public void listStreamThrowsExceptionWhenUserHasNoAccessToScope() {
        // Arrange
        createScopeAndStreams("scope1", Arrays.asList("stream1", "stream2", "stream3"),
                prepareFromFixedScaleTypePolicy(2));

        ControllerServiceBlockingStub stub = prepareCallStub(UserNames.SCOPE2_READ, DEFAULT_PASSWORD);
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope("scope1").build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act and assert
        AssertExtensions.assertThrows("Expected auth failure.",
                () -> stub.listStreamsInScope(request),
                e -> e.getMessage().contains("UNAUTHENTICATED"));
    }

    //region Private methods

    private static TxnId decode(UUID txnId) {
        Preconditions.checkNotNull(txnId, "txnId");
        return Controller.TxnId.newBuilder()
                .setHighBits(txnId.getMostSignificantBits())
                .setLowBits(txnId.getLeastSignificantBits())
                .build();
    }

    private TxnId createTransaction(StreamInfo streamInfo, int lease) {
        Preconditions.checkNotNull(streamInfo, "streamInfo");
        return createTransaction(UserNames.ADMIN, DEFAULT_PASSWORD, streamInfo, lease);
    }

    private TxnId createTransaction(String username, String password, StreamInfo streamInfo, int lease) {
        Exceptions.checkNotNullOrEmpty(username, "username");
        Exceptions.checkNotNullOrEmpty(password, "password");
        Preconditions.checkNotNull(streamInfo, "streamInfo");
        Controller.CreateTxnRequest request = Controller.CreateTxnRequest.newBuilder()
                .setStreamInfo(streamInfo)
                .setLease(lease)
                .build();

        Controller.CreateTxnResponse response = prepareCallStub(username, password).createTransaction(request);
        return response.getTxnId();
    }

    private SegmentId segmentId(String scope, String stream, long segmentId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(scope, "stream");
        return SegmentId.newBuilder()
                .setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(stream).build())
                .setSegmentId(segmentId)
                .build();
    }

    private ControllerServiceBlockingStub prepareCallStub(String username, String password) {
        Exceptions.checkNotNullOrEmpty(username, "username");
        Exceptions.checkNotNullOrEmpty(password, "password");

        ControllerServiceBlockingStub stub =
                ControllerServiceGrpc.newBlockingStub(inProcessChannel);

        // Set call credentials
        Credentials credentials = new DefaultCredentials(password, username);
        if (credentials != null) {
            PravegaCredentialsWrapper wrapper = new PravegaCredentialsWrapper(credentials);
            stub = stub.withCallCredentials(MoreCallCredentials.from(wrapper));
        }
        return stub;
    }

    private ScalingPolicy prepareFromFixedScaleTypePolicy(int numSegments) {
        return ScalingPolicy.newBuilder()
                .setScaleType(Controller.ScalingPolicy.ScalingPolicyType.FIXED_NUM_SEGMENTS)
                .setTargetRate(0)
                .setScaleFactor(0)
                .setMinNumSegments(numSegments)
                .build();
    }

    private void createScopeAndStream(String scopeName, String streamName, ScalingPolicy scalingPolicy) {
        createScopeAndStreams(scopeName, Arrays.asList(streamName), scalingPolicy);
    }

    private void createScopeAndStreams(String scopeName, List<String> streamNames, ScalingPolicy scalingPolicy) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scope");
        Preconditions.checkNotNull(streamNames, "stream");
        Preconditions.checkArgument(streamNames.size() > 0);
        Preconditions.checkNotNull(scalingPolicy, "scalingPolicy");

        ControllerServiceBlockingStub stub =
                prepareCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);
        createScope(stub, scopeName);

        streamNames.stream().forEach(n -> createStream(stub, scopeName, n, scalingPolicy));
    }

    private void createScope(ControllerServiceBlockingStub stub, String scopeName) {
        CreateScopeStatus status = stub.createScope(Controller.ScopeInfo.newBuilder().setScope(scopeName).build());
        if (!status.getStatus().equals(CreateScopeStatus.Status.SUCCESS)) {
            throw new RuntimeException("Failed to create scope");
        }
    }

    private void createStream(ControllerServiceBlockingStub stub, String scopeName,
                              String streamName, ScalingPolicy scalingPolicy) {
        StreamConfig streamConfig = StreamConfig.newBuilder()
                .setStreamInfo(Controller.StreamInfo.newBuilder()
                        .setScope(scopeName)
                        .setStream(streamName)
                        .build())
                .setScalingPolicy(scalingPolicy)
                .build();
        Controller.CreateStreamStatus status = stub.createStream(streamConfig);

        if (!status.getStatus().equals(Controller.CreateStreamStatus.Status.SUCCESS)) {
            throw new RuntimeException("Failed to create stream");
        }
    }

    private static File createAuthFile() {
        try {
            File result = File.createTempFile("auth_file", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

            try (FileWriter writer = new FileWriter(result.getAbsolutePath())) {
                String defaultPassword = passwordEncryptor.encryptPassword("1111_aaaa");
                writer.write(credentialsAndAclAsString(UserNames.ADMIN,  defaultPassword, "*,READ_UPDATE;"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE_READER, defaultPassword, "/,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_READ, defaultPassword, "scope1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE2_READ, defaultPassword, "scope2,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_READUPDATE, defaultPassword, "scope1/stream1,READ_UPDATE"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_READ, defaultPassword, "scope1/stream1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM2_READ, defaultPassword, "scope1/stream2,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_LIST_READ, defaultPassword, "scope1,READ;scope1/stream1,READ"));
            }
            return result;
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    //endregion

    /**
     * Holds username strings for use in the parent class.
     */
    private static class UserNames {
        private final static String ADMIN = "admin";
        private final static String SCOPE_READER = "scopereader";
        private final static String SCOPE1_READ = "scope1read";
        private final static String SCOPE2_READ = "scope2read";
        private final static String SCOPE1_STREAM1_READUPDATE = "authSc1Str1";
        private final static String SCOPE1_STREAM1_READ = "authSc1Str1readonly";
        private final static String SCOPE1_STREAM2_READ = "authSc1Str2readonly";
        private final static String SCOPE1_STREAM1_LIST_READ = "scope1stream1lr";
    }
}