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
package io.pravega.controller.server.rpc.auth;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.StatusRuntimeException;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.pravega.client.control.impl.ModelHelper;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.controller.server.eventProcessor.requesthandlers.AutoScaleTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.ScaleOperationTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.SealStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.StreamRequestHandler;
import io.pravega.controller.server.eventProcessor.requesthandlers.TruncateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateStreamTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.CreateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.UpdateReaderGroupTask;
import io.pravega.controller.server.eventProcessor.requesthandlers.DeleteScopeTask;
import io.pravega.shared.NameUtils;
import io.pravega.shared.security.auth.Credentials;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.client.control.impl.PravegaCredentialsWrapper;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Cluster;
import io.pravega.common.cluster.Host;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.metrics.StreamMetrics;
import io.pravega.controller.metrics.TransactionMetrics;
import io.pravega.controller.mocks.ControllerEventStreamWriterMock;
import io.pravega.controller.mocks.EventHelperMock;
import io.pravega.controller.mocks.EventStreamWriterMock;
import io.pravega.controller.mocks.SegmentHelperMock;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import io.pravega.controller.server.security.auth.handler.AuthInterceptor;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.authplugin.basic.PasswordAuthHandler;
import io.pravega.controller.server.rpc.grpc.v1.ControllerServiceImpl;
import io.pravega.controller.store.kvtable.KVTableMetadataStore;
import io.pravega.controller.store.kvtable.KVTableStoreFactory;
import io.pravega.controller.store.stream.AbstractStreamMetadataStore;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.task.TaskMetadataStore;
import io.pravega.controller.store.task.TaskStoreFactory;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScalingPolicy;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceBlockingStub;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc.ControllerServiceStub;
import io.pravega.controller.task.EventHelper;
import io.pravega.controller.task.KeyValueTable.TableMetadataTasks;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import io.pravega.test.common.AssertExtensions;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import static io.pravega.auth.AuthFileUtils.credentialsAndAclAsString;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
     * This rule is used later to expect both the exception class and the message.
     */

    private StreamMetadataTasks streamMetadataTasks;
    private StreamTransactionMetadataTasks streamTransactionMetadataTasks;
    private TableMetadataTasks kvtMetadataTasks;
    private KVTableMetadataStore kvtStore;
    private Server grpcServer;
    private Server grpcServerStrict;
    private ManagedChannel inProcessChannel;
    private ManagedChannel inProcessChannelStrict;

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
        StreamMetadataStore streamStore = StreamStoreFactory.createInMemoryStore();
        this.kvtStore = spy(KVTableStoreFactory.createInMemoryStore(streamStore, EXECUTOR));

        BucketStore bucketStore = StreamStoreFactory.createInMemoryBucketStore();
        SegmentHelper segmentHelper = SegmentHelperMock.getSegmentHelperMock();
        RequestTracker requestTracker = new RequestTracker(true);
        StreamMetrics.initialize();
        TransactionMetrics.initialize();

        GrpcAuthHelper authHelper = new GrpcAuthHelper(true, "secret", 300);
        EventHelper helper = EventHelperMock.getEventHelperMock(EXECUTOR, "host", ((AbstractStreamMetadataStore) streamStore).getHostTaskIndex());
        streamMetadataTasks = new StreamMetadataTasks(streamStore, bucketStore, taskMetadataStore, segmentHelper,
                EXECUTOR, "host", authHelper, helper);

        streamTransactionMetadataTasks = new StreamTransactionMetadataTasks(streamStore, segmentHelper,
                EXECUTOR, "host", authHelper);

        kvtMetadataTasks = new TableMetadataTasks(kvtStore,  segmentHelper,
                EXECUTOR, EXECUTOR, "host", authHelper, helper);

        StreamRequestHandler streamRequestHandler = new StreamRequestHandler(new AutoScaleTask(streamMetadataTasks, 
                streamStore, EXECUTOR),
                new ScaleOperationTask(streamMetadataTasks, streamStore, EXECUTOR),
                new UpdateStreamTask(streamMetadataTasks, streamStore, bucketStore, EXECUTOR),
                new SealStreamTask(streamMetadataTasks, streamTransactionMetadataTasks, streamStore, EXECUTOR),
                new DeleteStreamTask(streamMetadataTasks, streamStore, bucketStore, EXECUTOR),
                new TruncateStreamTask(streamMetadataTasks, streamStore, EXECUTOR),
                new CreateReaderGroupTask(streamMetadataTasks, streamStore, EXECUTOR),
                new DeleteReaderGroupTask(streamMetadataTasks, streamStore, EXECUTOR),
                new UpdateReaderGroupTask(streamMetadataTasks, streamStore, EXECUTOR),
                streamStore,
                new DeleteScopeTask(streamMetadataTasks, streamStore, kvtStore, kvtMetadataTasks, EXECUTOR),
                EXECUTOR);

        streamMetadataTasks.setRequestEventWriter(new ControllerEventStreamWriterMock(streamRequestHandler, EXECUTOR));
        streamTransactionMetadataTasks.initializeStreamWriters(new EventStreamWriterMock<>(), new EventStreamWriterMock<>());

        Cluster mockCluster = mock(Cluster.class);
        when(mockCluster.getClusterMembers()).thenReturn(Collections.singleton(new Host("localhost", 9090, null)));

        ControllerServiceGrpc.ControllerServiceImplBase controllerServiceImplBase = new ControllerServiceImpl(
                new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore,
                                      streamMetadataTasks,
                                      streamTransactionMetadataTasks,
                                      segmentHelper,
                                      EXECUTOR,
                                      mockCluster, requestTracker),
                authHelper, requestTracker, true, true, 2);

        ControllerServiceGrpc.ControllerServiceImplBase controllerServiceImplBaseStrict = new ControllerServiceImpl(
                new ControllerService(kvtStore, kvtMetadataTasks, streamStore, bucketStore,
                        streamMetadataTasks,
                        streamTransactionMetadataTasks,
                        segmentHelper,
                        EXECUTOR,
                        mockCluster, requestTracker),
                authHelper, requestTracker, true, false, 2);

        PasswordAuthHandler authHandler = new PasswordAuthHandler();
        authHandler.initialize(AUTH_FILE.getAbsolutePath());

        String uniqueServerName = String.format("Test server name: %s", getClass());
        String uniqueServerNameStrict = String.format("Test server name: %sStrict", getClass());

        // Using a builder that creates a server for servicing in-process requests.
        // Also, using a direct executor which executes app code directly in transport thread. See
        // https://grpc.io/grpc-java/javadoc/io/grpc/inprocess/InProcessServerBuilder.html for more information.
        grpcServer = InProcessServerBuilder.forName(uniqueServerName)
                .addService(ServerInterceptors.intercept(controllerServiceImplBase,
                        new AuthInterceptor(authHandler)))
                .directExecutor()
                .build()
                .start();
        grpcServerStrict = InProcessServerBuilder.forName(uniqueServerNameStrict)
                .addService(ServerInterceptors.intercept(controllerServiceImplBaseStrict,
                        new AuthInterceptor(authHandler)))
                .directExecutor()
                .build()
                .start();
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
        inProcessChannelStrict = InProcessChannelBuilder.forName(uniqueServerNameStrict).directExecutor().build();
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
        inProcessChannelStrict.shutdownNow();
        grpcServer.shutdownNow();
        grpcServerStrict.shutdownNow();
        StreamMetrics.reset();
        TransactionMetrics.reset();
    }

    @Test(timeout = 20000)
    public void createScopeSucceedsForPrivilegedUser() {
        //Arrange
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        //Act
        CreateScopeStatus status = blockingStub.createScope(Controller.ScopeInfo.newBuilder().setScope("dummy").build());

        //Verify
        assertEquals(CreateScopeStatus.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void createScopeFailsForUnauthorizedUser() {
        //Arrange
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER, DEFAULT_PASSWORD);

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class, () -> blockingStub.createScope(Controller.ScopeInfo.newBuilder().setScope("dummy").build()));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void createScopeFailsForNonExistentUser() {
        //Arrange
        ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub("whatever", "whatever");

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.createScope(Controller.ScopeInfo.newBuilder().setScope("dummy").build()));
        assertTrue(exception.getMessage().contains("UNAUTHENTICATED"));

    }

    @Test(timeout = 20000)
    public void createReaderGroupSucceedsForPrivilegedUserInStrictCase() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        createScopeAndStreamStrict(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStubStrict(UserNames.ADMIN, DEFAULT_PASSWORD);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group", ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Act
        Controller.CreateReaderGroupResponse status = blockingStub.createReaderGroup(config);

        //Verify
        assertEquals(Controller.CreateReaderGroupResponse.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void createReaderGroupSucceedsForLowerPrivilegedUser() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group", ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Act
        Controller.CreateReaderGroupResponse status = blockingStub.createReaderGroup(config);

        //Verify
        assertEquals(Controller.CreateReaderGroupResponse.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void createReaderGroupFailsForLowerPrivilegedUserInStrictCase() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        createScopeAndStreamStrict(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStubStrict(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group", ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.createReaderGroup(config));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void createReaderGroupFailsForUnauthorizedUser() {
        //Arrange
        String scope = "test";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER, DEFAULT_PASSWORD);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group", ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.createReaderGroup(config));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void createReaderGroupFailsForNonExistentUser() {
        //Arrange
        String scope = "test";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub("whatever", "whatever");
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group", ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.createReaderGroup(config));
        assertTrue(exception.getMessage().contains("UNAUTHENTICATED"));
    }

    @Test(timeout = 20000)
    public void deleteReaderGroupSucceedsForPrivilegedUserInStrictCase() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        String readerGroup = "group";
        ReaderGroupConfig config = ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build();
        config = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        createScopeAndStreamStrict(scope, stream, prepareFromFixedScaleTypePolicy(2));
        createReaderGroupStrict(scope, readerGroup, config);
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStubStrict(UserNames.ADMIN, DEFAULT_PASSWORD);

        //Act
        Controller.DeleteReaderGroupStatus status = blockingStub.deleteReaderGroup(ModelHelper.createReaderGroupInfo(scope,
                readerGroup, config.getReaderGroupId().toString(), 0));

        //Verify
        assertEquals(Controller.DeleteReaderGroupStatus.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void deleteReaderGroupSucceedsForLowerPrivilegedUser() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        String readerGroup = "group";
        ReaderGroupConfig config = ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build();
        config = ReaderGroupConfig.cloneConfig(config, UUID.randomUUID(), 0L);
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        createReaderGroup(scope, readerGroup, config);
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);

        //Act
        Controller.DeleteReaderGroupStatus status = blockingStub.deleteReaderGroup(ModelHelper.createReaderGroupInfo(scope,
                readerGroup, config.getReaderGroupId().toString(), 0));

        //Verify
        assertEquals(Controller.DeleteReaderGroupStatus.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void deleteReaderGroupFailsForLowerPrivilegedUserInStrictCase() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        createScopeAndStreamStrict(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStubStrict(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.deleteReaderGroup(ModelHelper.createReaderGroupInfo(scope,
                        "group", UUID.randomUUID().toString(), 0)));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void deleteReaderGroupFailsForUnauthorizedUser() {
        //Arrange
        String scope = "test";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER, DEFAULT_PASSWORD);

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.deleteReaderGroup(ModelHelper.createReaderGroupInfo(scope,
                        "group", UUID.randomUUID().toString(), 0)));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void deleteReaderGroupFailsForNonExistentUser() {
        //Arrange
        String scope = "test";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub("whatever", "whatever");

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.deleteReaderGroup(ModelHelper.createReaderGroupInfo(scope,
                        "group", UUID.randomUUID().toString(), 0)));
        assertTrue(exception.getMessage().contains("UNAUTHENTICATED"));
    }

    @Test(timeout = 20000)
    public void updateReaderGroupSucceedsForPrivilegedUserInStrictCase() {
        //Arrange
        String scope = "scope1";
        String stream1 = "test1";
        String stream2 = "test2";
        String readerGroup = "group";
        final UUID rgId = UUID.randomUUID();
        ReaderGroupConfig oldConfig = ReaderGroupConfig.builder()
                .stream(NameUtils.getScopedStreamName(scope, stream1)).build();
        oldConfig = ReaderGroupConfig.cloneConfig(oldConfig, rgId, 0L);
        createScopeAndStreamsStrict(scope, Arrays.asList(stream1, stream2), prepareFromFixedScaleTypePolicy(2));
        createReaderGroupStrict(scope, readerGroup, oldConfig);
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStubStrict(UserNames.ADMIN, DEFAULT_PASSWORD);
        ReaderGroupConfig rgConf = ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream2)).build();
        rgConf = ReaderGroupConfig.cloneConfig(rgConf, oldConfig.getReaderGroupId(), 0L);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, readerGroup, rgConf);

        //Act
        Controller.UpdateReaderGroupResponse status = blockingStub.updateReaderGroup(config);

        //Verify
        assertEquals(Controller.UpdateReaderGroupResponse.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void updateReaderGroupSucceedsForLowerPrivilegedUser() {
        //Arrange
        String scope = "scope1";
        String stream1 = "test1";
        String stream2 = "test2";
        String readerGroup = "group";
        UUID rgId = UUID.randomUUID();
        ReaderGroupConfig oldConfig = ReaderGroupConfig.builder()
                .stream(NameUtils.getScopedStreamName(scope, stream1)).build();
        oldConfig = ReaderGroupConfig.cloneConfig(oldConfig, rgId, 0L);
        createScopeAndStreams(scope, Arrays.asList(stream1, stream2), prepareFromFixedScaleTypePolicy(2));
        createReaderGroup(scope, readerGroup, oldConfig);
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);
        ReaderGroupConfig rgConf = ReaderGroupConfig.builder()
                .stream(NameUtils.getScopedStreamName(scope, stream2)).build();
        rgConf = ReaderGroupConfig.cloneConfig(rgConf, rgId, 0L);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, readerGroup, rgConf);

        //Act
        Controller.UpdateReaderGroupResponse status = blockingStub.updateReaderGroup(config);

        //Verify
        assertEquals(Controller.UpdateReaderGroupResponse.Status.SUCCESS, status.getStatus());
    }

    @Test(timeout = 20000)
    public void updateReaderGroupFailsForLowerPrivilegedUserInStrictCase() {
        //Arrange
        String scope = "scope1";
        String stream = "test";
        UUID rgId = UUID.randomUUID();
        createScopeAndStreamStrict(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStubStrict(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);
        ReaderGroupConfig rgConfig = ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream))
                .build();
        rgConfig = ReaderGroupConfig.cloneConfig(rgConfig, rgId, 0L);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group", rgConfig);

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.updateReaderGroup(config));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void updateReaderGroupFailsForUnauthorizedUser() {
        //Arrange
        String scope = "test";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub(UserNames.SCOPE_READER, DEFAULT_PASSWORD);
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group",
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.updateReaderGroup(config));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void updateReaderGroupFailsForNonExistentUser() {
        //Arrange
        String scope = "test";
        String stream = "test";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub =
                prepareBlockingCallStub("whatever", "whatever");
        Controller.ReaderGroupConfiguration config = ModelHelper.decode(scope, "group",
                ReaderGroupConfig.builder().stream(NameUtils.getScopedStreamName(scope, stream)).build());

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> blockingStub.updateReaderGroup(config));
        assertTrue(exception.getMessage().contains("UNAUTHENTICATED"));
    }

    @Test(timeout = 20000)
    public void getUriSucceedsForPrivilegedUser() {
        String scope = "scope1";
        String stream = "stream1";

        //Arrange
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));

        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        //Act
        NodeUri nodeUri1 = stub.getURI(segmentId(scope, stream, 0));
        NodeUri nodeUri2 = stub.getURI(segmentId(scope, stream, 1));

        //Verify
        assertEquals("localhost", nodeUri1.getEndpoint());
        assertEquals(12345, nodeUri1.getPort());
        assertEquals("localhost", nodeUri2.getEndpoint());
        assertEquals(12345, nodeUri2.getPort());
    }

    @Test(timeout = 20000)
    public void getUriFailsForNonExistentUser() {
        String scope = "scope1";
        String stream = "stream1";

        //Arrange
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareBlockingCallStub("nonexistentuser", "whatever");

        //Verify & Act
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> stub.getURI(segmentId(scope, stream, 0)));
        assertTrue(exception.getMessage().contains("UNAUTHENTICATED"));
    }

    @Test(timeout = 20000)
    public void isSegmentValidSucceedsForAuthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE1_STREAM1_READ, DEFAULT_PASSWORD);

        assertTrue(stub.isSegmentValid(segmentId(scope, stream, 0)).getResponse());
        assertFalse(stub.isSegmentValid(segmentId(scope, stream, 3)).getResponse());
    }

    @Test(timeout = 20000)
    public void isSegmentValidFailsForUnauthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";
        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));

        //Note that the user has READ access to scope1/stream2, not scope1/stream1.
        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE1_STREAM2_READ, DEFAULT_PASSWORD);

        //Set the expected exception
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> stub.isSegmentValid(segmentId(scope, stream, 0)));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void pingTransactionSucceedsForAuthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";

        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        TxnId transactionId = createTransaction(StreamInfo.newBuilder().setScope(scope).setStream(stream).build(), 2000);

        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE1_STREAM1_READUPDATE, DEFAULT_PASSWORD);

        PingTxnStatus status = stub.pingTransaction(Controller.PingTxnRequest.newBuilder()
                    .setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(stream).build())
                    .setTxnId(transactionId)
                    .setLease(1000)
                    .build());
        assertEquals(PingTxnStatus.Status.OK, status.getStatus());
    }

    @Test(timeout = 20000)
    public void pingTransactionFailsForUnAuthorizedUser() {
        String scope = "scope1";
        String stream = "stream1";

        createScopeAndStream(scope, stream, prepareFromFixedScaleTypePolicy(2));
        TxnId transactionId = createTransaction(StreamInfo.newBuilder().setScope(scope).setStream(stream).build(), 2000);

        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE1_STREAM1_READ, DEFAULT_PASSWORD);

        //Set the expected exception
        StatusRuntimeException exception = Assert.assertThrows(StatusRuntimeException.class,
                () -> stub.pingTransaction(Controller.PingTxnRequest.newBuilder()
                        .setStreamInfo(StreamInfo.newBuilder().setScope(scope).setStream(stream).build())
                        .setTxnId(transactionId)
                        .setLease(1000)
                        .build()));
        assertTrue(exception.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void listScopes() {
        // Arrange
        ControllerServiceBlockingStub stub =
                prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);
        createScope(stub, "scope1");
        createScope(stub, "scope2");
        createScope(stub, "scope3");
        createScope(stub, "scope4");

        stub = prepareBlockingCallStub(UserNames.SCOPE_READER1_READ, DEFAULT_PASSWORD);
        Controller.ScopesRequest request = Controller.ScopesRequest
                .newBuilder()
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        Controller.ScopesResponse response = stub.listScopes(request);

        // Assert
        assertEquals(1, response.getScopesList().size());
        assertEquals("4", response.getContinuationToken().getToken());

        stub = prepareBlockingCallStub(UserNames.SCOPE_READER1_3_READ, DEFAULT_PASSWORD);
        request = Controller.ScopesRequest
                .newBuilder()
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        response = stub.listScopes(request);

        // Assert
        assertEquals(2, response.getScopesList().size());
        assertEquals("3", response.getContinuationToken().getToken());
    }

    @Test(timeout = 20000)
    public void listStreamsReturnsAllWhenUserHasWildCardAccessUsingBlockingStub() {
        // Arrange
        String scopeName = "scope1";
        createScopeAndStreams(scopeName, Arrays.asList("stream1", "stream2"),
                prepareFromFixedScaleTypePolicy(2));

        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scopeName).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();
        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        // Act
        Controller.StreamsInScopeResponse response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(2, response.getStreamsList().size());
    }

    @Test(timeout = 20000)
    public void listStreamsReturnsAllWhenUserHasWildCardAccessUsingAsyncStub() {
        // Arrange
        String scopeName = "scope1";
        createScopeAndStreams(scopeName, Arrays.asList("stream1", "stream2"),
                prepareFromFixedScaleTypePolicy(2));

        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scopeName).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        ControllerServiceStub stub = prepareNonBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);
        ResultObserver<Controller.StreamsInScopeResponse> responseObserver = new ResultObserver<>();
        stub.listStreamsInScope(request, responseObserver);
        List<Controller.StreamInfo> streamsInResponse = responseObserver.get().getStreamsList();

        assertFalse(Strings.isNullOrEmpty(responseObserver.get().getContinuationToken().getToken()));
        assertEquals(2, streamsInResponse.size());
    }

    @Test(timeout = 20000)
    public void listStreamReturnsEmptyResultWhenUserHasNoAccessToStreams() {
        // Arrange
        createScopeAndStreams("scope1", Arrays.asList("stream1", "stream2", "stream3"),
                prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE1_READ, DEFAULT_PASSWORD);
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope("scope1").build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        Controller.StreamsInScopeResponse response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(0, response.getStreamsList().size());
    }

    @Test(timeout = 20000)
    public void listStreamFiltersResultWhenUserHasAccessToSubsetOfStreams() {
        // Arrange
        String scope = "scope1";
        createScopeAndStreams(scope, Arrays.asList("stream1", "stream2", "stream3", "stream4"),
                prepareFromFixedScaleTypePolicy(2));
        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE1_STREAM1_LIST_READ, DEFAULT_PASSWORD);
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scope).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        Controller.StreamsInScopeResponse response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(1, response.getStreamsList().size());

        stub = prepareBlockingCallStub(UserNames.SCOPE1_STREAM1_3_LIST_READ, DEFAULT_PASSWORD);
        request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scope).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        response = stub.listStreamsInScope(request);

        // Assert
        assertEquals(2, response.getStreamsList().size());
    }

    @Test(timeout = 20000)
    public void listStreamThrowsExceptionWhenUserHasNoAccessToScope() {
        // Arrange
        createScopeAndStreams("scope1", Arrays.asList("stream1", "stream2", "stream3"),
                prepareFromFixedScaleTypePolicy(2));

        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.SCOPE2_READ, DEFAULT_PASSWORD);
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope("scope1").build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act and assert
        AssertExtensions.assertThrows("Expected auth failure.",
                () -> stub.listStreamsInScope(request),
                e -> e.getMessage().contains("PERMISSION_DENIED"));
    }

    @Test(timeout = 20000)
    public void listStreamThrowsExceptionWhenUserIsNonExistent() {
        // Arrange
        createScopeAndStreams("scope1", Arrays.asList("stream1", "stream2", "stream3"),
                prepareFromFixedScaleTypePolicy(2));

        ControllerServiceBlockingStub stub = prepareBlockingCallStubWithNoCredentials();
        Controller.StreamsInScopeRequest request = Controller.StreamsInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope("scope1").build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act and assert
        AssertExtensions.assertThrows("Expected auth failure.",
                () -> stub.listStreamsInScope(request),
                e -> e.getMessage().contains("UNAUTHENTICATED"));
    }

    @Test(timeout = 20000)
    public void listKVTFiltersResultWhenUserHasAccessToSubsetOfTables() {
        // Arrange
        String scope = "scope1";
        ControllerServiceBlockingStub stub = prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);
        createScope(stub, scope);

        doAnswer(x -> CompletableFuture.completedFuture(true)).when(this.kvtStore).checkScopeExists(any(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(new ImmutablePair<>(Lists.newArrayList("table1", "table2"), "2")))
                .when(this.kvtStore).listKeyValueTables(anyString(), 
                eq(""), anyInt(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(new ImmutablePair<>(Lists.newArrayList("table3", "table4"), "4")))
                .when(this.kvtStore).listKeyValueTables(anyString(), 
                eq("2"), anyInt(), any(), any());
        doAnswer(x -> CompletableFuture.completedFuture(new ImmutablePair<>(Collections.emptyList(), "4")))
                .when(this.kvtStore).listKeyValueTables(anyString(), 
                eq("4"), anyInt(), any(), any());
        
        stub = prepareBlockingCallStub(UserNames.SCOPE1_TABLE1_LIST_READ, DEFAULT_PASSWORD);
        Controller.KVTablesInScopeRequest request = Controller.KVTablesInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scope).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        Controller.KVTablesInScopeResponse response = stub.listKeyValueTablesInScope(request);

        // Assert
        assertEquals(1, response.getKvtablesCount());

        stub = prepareBlockingCallStub(UserNames.SCOPE1_TABLE1_3_LIST_READ, DEFAULT_PASSWORD);
        request = Controller.KVTablesInScopeRequest
                .newBuilder().setScope(
                        Controller.ScopeInfo.newBuilder().setScope(scope).build())
                .setContinuationToken(Controller.ContinuationToken.newBuilder().build()).build();

        // Act
        response = stub.listKeyValueTablesInScope(request);

        // Assert
        assertEquals(2, response.getKvtablesCount());
    }

    //region Private methods

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

        Controller.CreateTxnResponse response = prepareBlockingCallStub(username, password).createTransaction(request);
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

    private ControllerServiceBlockingStub prepareBlockingCallStubStrict(String username, String password) {
        Exceptions.checkNotNullOrEmpty(username, "username");
        Exceptions.checkNotNullOrEmpty(password, "password");

        ControllerServiceBlockingStub stub =
                ControllerServiceGrpc.newBlockingStub(inProcessChannelStrict);

        // Set call credentials
        Credentials credentials = new DefaultCredentials(password, username);
        if (credentials != null) {
            PravegaCredentialsWrapper wrapper = new PravegaCredentialsWrapper(credentials);
            stub = stub.withCallCredentials(MoreCallCredentials.from(wrapper));
        }
        return stub;
    }

    private ControllerServiceBlockingStub prepareBlockingCallStubWithNoCredentials() {
        return ControllerServiceGrpc.newBlockingStub(inProcessChannel);
    }

    private ControllerServiceBlockingStub prepareBlockingCallStub(String username, String password) {
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

    private ControllerServiceStub prepareNonBlockingCallStub(String username, String password) {
        Exceptions.checkNotNullOrEmpty(username, "username");
        Exceptions.checkNotNullOrEmpty(password, "password");

        ControllerServiceGrpc.ControllerServiceStub stub = ControllerServiceGrpc.newStub(inProcessChannel);

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

    private void createScopeAndStreamStrict(String scopeName, String streamName, ScalingPolicy scalingPolicy) {
        createScopeAndStreamsStrict(scopeName, Arrays.asList(streamName), scalingPolicy);
    }

    private void createScopeAndStreamsStrict(String scopeName, List<String> streamNames, ScalingPolicy scalingPolicy) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scope");
        Preconditions.checkNotNull(streamNames, "stream");
        Preconditions.checkArgument(streamNames.size() > 0);
        Preconditions.checkNotNull(scalingPolicy, "scalingPolicy");

        ControllerServiceBlockingStub stub =
                prepareBlockingCallStubStrict(UserNames.ADMIN, DEFAULT_PASSWORD);
        createScope(stub, scopeName);

        streamNames.stream().forEach(n -> createStream(stub, scopeName, n, scalingPolicy));
    }

    private void createScopeAndStreams(String scopeName, List<String> streamNames, ScalingPolicy scalingPolicy) {
        Exceptions.checkNotNullOrEmpty(scopeName, "scope");
        Preconditions.checkNotNull(streamNames, "stream");
        Preconditions.checkArgument(streamNames.size() > 0);
        Preconditions.checkNotNull(scalingPolicy, "scalingPolicy");

        ControllerServiceBlockingStub stub =
                prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);
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

    private void createReaderGroup(String scope, String group, ReaderGroupConfig config) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(group, "group name");
        Preconditions.checkNotNull(config, "ReaderGroupConfig");
        ControllerServiceBlockingStub stub =
                prepareBlockingCallStub(UserNames.ADMIN, DEFAULT_PASSWORD);

        Controller.CreateReaderGroupResponse status = stub.createReaderGroup(ModelHelper.decode(scope, group, config));
        if (!status.getStatus().equals(Controller.CreateReaderGroupResponse.Status.SUCCESS)) {
            throw new RuntimeException("Failed to create reader-group");
        }
    }

    private void createReaderGroupStrict(String scope, String group, ReaderGroupConfig config) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(group, "group name");
        Preconditions.checkNotNull(config, "ReaderGroupConfig");
        ControllerServiceBlockingStub stub =
                prepareBlockingCallStubStrict(UserNames.ADMIN, DEFAULT_PASSWORD);

        Controller.CreateReaderGroupResponse status = stub.createReaderGroup(ModelHelper.decode(scope, group, config));
        if (!status.getStatus().equals(Controller.CreateReaderGroupResponse.Status.SUCCESS)) {
            throw new RuntimeException("Failed to create reader-group");
        }
    }

    private static File createAuthFile() {
        try {
            File result = File.createTempFile("auth_file", ".txt");
            StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

            try (FileWriter writer = new FileWriter(result.getAbsolutePath())) {
                String defaultPassword = passwordEncryptor.encryptPassword("1111_aaaa");
                writer.write(credentialsAndAclAsString(UserNames.ADMIN,  defaultPassword, "prn::*,READ_UPDATE;"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE_READER, defaultPassword, "prn::/,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE_READER1_READ, defaultPassword, "prn::/,READ;prn::/scope:scope1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE_READER1_3_READ, defaultPassword, "prn::/,READ;prn::/scope:scope1,READ;prn::/scope:scope3,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_READ, defaultPassword, "prn::/scope:scope1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE2_READ, defaultPassword, "prn::/scope:scope2,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_READUPDATE, defaultPassword, "prn::/scope:scope1/stream:stream1,READ_UPDATE"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_READ, defaultPassword, "prn::/scope:scope1/stream:stream1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM2_READ, defaultPassword, "prn::/scope:scope1/stream:stream2,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_LIST_READ, defaultPassword,
                        "prn::/scope:scope1,READ;prn::/scope:scope1/stream:stream1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_STREAM1_3_LIST_READ, defaultPassword,
                        "prn::/scope:scope1,READ;prn::/scope:scope1/stream:stream1,READ;prn::/scope:scope1/stream:stream3,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_TABLE1_LIST_READ, defaultPassword,
                        "prn::/scope:scope1,READ;prn::/scope:scope1/key-value-table:table1,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_TABLE1_3_LIST_READ, defaultPassword,
                        "prn::/scope:scope1,READ;prn::/scope:scope1/key-value-table:table1,READ;prn::/scope:scope1/key-value-table:table3,READ"));
                writer.write(credentialsAndAclAsString(UserNames.SCOPE1_READ, defaultPassword, "prn::/scope:scope1,READ"));
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
        private final static String SCOPE_READER1_READ = "scopereader1read";
        private final static String SCOPE_READER1_3_READ = "scopereader1_3read";
        private final static String SCOPE1_READ = "scope1read";
        private final static String SCOPE2_READ = "scope2read";
        private final static String SCOPE1_STREAM1_READUPDATE = "authSc1Str1";
        private final static String SCOPE1_STREAM1_READ = "authSc1Str1readonly";
        private final static String SCOPE1_STREAM2_READ = "authSc1Str2readonly";
        private final static String SCOPE1_STREAM1_LIST_READ = "scope1stream1lr";
        private final static String SCOPE1_STREAM1_3_LIST_READ = "scope1stream1_3lr";
        private final static String SCOPE1_TABLE1_LIST_READ = "scope1table1lr";
        private final static String SCOPE1_TABLE1_3_LIST_READ = "scope1table1_3lr";
    }

    static class ResultObserver<T> implements StreamObserver<T> {
        private T result = null;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            future.complete(result);
        }

        public T get() {
            return future.join();
        }
    }
}
