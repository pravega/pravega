/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import com.google.common.collect.ImmutableMap;
import io.grpc.ServerBuilder;
import io.pravega.client.ClientConfig;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.eventProcessor.LocalController;
import io.pravega.controller.server.rest.RESTServer;
import io.pravega.controller.server.rest.RESTServerConfig;
import io.pravega.controller.server.rest.generated.model.CreateScopeRequest;
import io.pravega.controller.server.rest.generated.model.ScopesList;
import io.pravega.controller.server.rest.generated.model.StreamState;
import io.pravega.controller.server.rest.generated.model.StreamsList;
import io.pravega.controller.server.rest.impl.RESTServerConfigImpl;
import io.pravega.controller.server.rpc.auth.PravegaAuthManager;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.test.common.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * There are authorization-related tests elsewhere ({@link SecureStreamMetaDataTests} and
 * {@link UserSecureStreamMetaDataTests}) too. Here, we have focused authorization tests which test the logic more
 * comprehensively; Tests here also run much quicker, since we share REST server, auth handler and its configuration
 * across all tests in this class.
 */
public class AuthLogicFocusedTests {

    private final static int HTTP_STATUS_OK = 200;
    private final static int HTTP_STATUS_NOCONTENT = 204;
    private final static int HTTP_STATUS_UNAUTHORIZED = 401;
    private final static int HTTP_STATUS_FORBIDDEN = 403;

    // Suppressing the checkstyle errors below, as we are using a class initializer (a method with @BeforeClass
    // annotation) for efficiency, and we cannot make these members final.

    @SuppressWarnings("checkstyle:StaticVariableName")
    private static ControllerService mockControllerService;

    @SuppressWarnings("checkstyle:StaticVariableName")
    private static RESTServerConfig serverConfig;

    @SuppressWarnings("checkstyle:StaticVariableName")
    private static RESTServer restServer;

    @SuppressWarnings("checkstyle:StaticVariableName")
    private static Client client;

    @SuppressWarnings("checkstyle:StaticVariableName")
    private static File passwordHandlerInputFile;

    //region Test class initializer and cleanup

    @BeforeClass
    public static void initializer() throws IOException, InvalidKeySpecException, NoSuchAlgorithmException {

        passwordHandlerInputFile = File.createTempFile("AuthFocusedTests", ".txt");

        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

        try (FileWriter writer = new FileWriter(passwordHandlerInputFile.getAbsolutePath())) {
            String encryptedPassword = passwordEncryptor.encryptPassword("1111_aaaa");

            // Admin has READ_WRITE permission to everything
            writer.write("privilegedUser:" + encryptedPassword + ":*,READ_UPDATE\n");

            writer.write("user1:" + encryptedPassword + ":/,READ_UPDATE;scope1,READ_UPDATE;scope2,READ_UPDATE;\n");

            writer.write("unauthorizedUser" + encryptedPassword + ":/,READ_UPDATE;scope1,READ_UPDATE;scope2,READ_UPDATE;\n");
            writer.write("userAccessToSubsetOfScopes:" + encryptedPassword + ":/,READ;scope3,READ_UPDATE;\n");
            writer.write("userWithNoAuthorizations:" + encryptedPassword + ":;\n");
            writer.write("userWithDeletePermission:" + encryptedPassword + ":scopeToDelete,READ_UPDATE;\n");
            writer.write("userAuthOnScopeButNotOnStreams:" + encryptedPassword + ":myscope,READ_UPDATE;\n");
            writer.write("userAuthOnScopeAndReadOnAllStreams:" + encryptedPassword
                    + ":myscope,READ_UPDATE;myscope/*,READ;\n");
            writer.write("userAuthOnScopeAndWriteOnAllStreams:" + encryptedPassword + ":myscope,READ_UPDATE;myscope/*,READ_UPDATE;\n");
            writer.write("userAuthOnScopeAndWriteOnSpecificStream:" + encryptedPassword
                    + ":myscope,READ_UPDATE;myscope/stream1,READ_UPDATE;\n");
        }

        PravegaAuthManager authManager = new PravegaAuthManager(GRPCServerConfigImpl.builder()
                .authorizationEnabled(true)
                .userPasswordFile(passwordHandlerInputFile.getAbsolutePath())
                .port(1000)
                .build());
        ServerBuilder<?> server = ServerBuilder.forPort(TestUtils.getAvailableListenPort());
        authManager.registerInterceptors(server);

        mockControllerService = mock(ControllerService.class);
        serverConfig = RESTServerConfigImpl.builder().host("localhost").port(TestUtils.getAvailableListenPort()).build();
        LocalController controller = new LocalController(mockControllerService, false, "");
        restServer = new RESTServer(controller, mockControllerService, authManager, serverConfig,
                new ConnectionFactoryImpl(ClientConfig.builder()
                        .controllerURI(URI.create("tcp://localhost"))
                        .build()));
        restServer.startAsync();
        restServer.awaitRunning();
        client = ClientBuilder.newClient();
    }

    @AfterClass
    public static void cleanup() {
        if (restServer != null && restServer.isRunning()) {
            restServer.stopAsync();
            try {
                restServer.awaitTerminated(2, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // ignore
            }
        }
        if (passwordHandlerInputFile != null) {
            passwordHandlerInputFile.delete();
        }
    }

    //endregion

    @Test
    public void testListScopesReturnsFilteredResults() throws ExecutionException, InterruptedException {
        // Arrange
        final String resourceURI = getURI() + "v1/scopes";
        when(mockControllerService.listScopes()).thenReturn(CompletableFuture.completedFuture(
                Arrays.asList("scope1", "scope2", "scope3")));
        Invocation requestInvocation = this.invocationBuilder(resourceURI, "userAccessToSubsetOfScopes", "1111_aaaa")
                .buildGet();

        // Act
        Response response = requestInvocation.invoke();
        ScopesList scopes = response.readEntity(ScopesList.class);

        // Assert
        assertEquals(1, scopes.getScopes().size());
        assertEquals("scope3", scopes.getScopes().get(0).getScopeName());

        response.close();
    }

    @Test
    public void testListScopesReturnsUnauthorizedStatusForInvalidUser() {
        // Arrange
        final String resourceURI = getURI() + "v1/scopes";
        when(mockControllerService.listScopes()).thenReturn(CompletableFuture.completedFuture(
                Arrays.asList("scope1", "scope2", "scope3")));
        Invocation requestInvocation = this.invocationBuilder(resourceURI, "fictitiousUser", "whatever")
                .buildGet();

        // Act
        Response response = requestInvocation.invoke();

        // Assert
        assertEquals(HTTP_STATUS_UNAUTHORIZED, response.getStatus());

        response.close();
    }

    @Test
    public void testListScopesIsForbiddenForValidButUnauthorizedUser() {
        // Arrange
        final String resourceURI = getURI() + "v1/scopes";
        when(mockControllerService.listScopes()).thenReturn(CompletableFuture.completedFuture(
                Arrays.asList("scope1", "scope2", "scope3")));
        Invocation requestInvocation = this.invocationBuilder(resourceURI,
                "userWithNoAuthorizations", "1111_aaaa")
                .buildGet();

        // Act
        Response response = requestInvocation.invoke();

        // Assert
        assertEquals(HTTP_STATUS_FORBIDDEN, response.getStatus());

        response.close();
    }

    @Test
    public void testDeleteScopeSucceedsForAuthorizedUser() {
        // Arrange
        String scopeName = "scopeToDelete";

        createScope(scopeName, "admin", "1111_aaaa");

        final String resourceUri = getURI() + "v1/scopes/" + scopeName;
        when(mockControllerService.deleteScope(scopeName)).thenReturn(
                CompletableFuture.completedFuture(
                        Controller.DeleteScopeStatus.newBuilder().setStatus(
                                Controller.DeleteScopeStatus.Status.SUCCESS).build()));

        // Act
        Response response = invocationBuilder(resourceUri, "userWithDeletePermission", "1111_aaaa")
                .buildDelete().invoke();

        // Asert
        assertEquals(HTTP_STATUS_NOCONTENT, response.getStatus());
        response.close();
    }

    @Test
    public void testDeleteScopeSucceedsForPrivilegedUser() {
        // Arrange

        String scopeName = "scopeForAdminToDelete";

        // The special thing about this user is that the user is assigned a wildcard permission: "*,READ_UPDATE"
        String userName = "privilegedUser";
        String password = "1111_aaaa";

        createScope(scopeName, userName, password);
        final String resourceURI = getURI() + "v1/scopes/" + scopeName;

        when(mockControllerService.deleteScope(scopeName)).thenReturn(
                CompletableFuture.completedFuture(
                        Controller.DeleteScopeStatus.newBuilder().setStatus(
                                Controller.DeleteScopeStatus.Status.SUCCESS).build()));

        // Act
        Response response = this.invocationBuilder(resourceURI, userName, password).buildDelete().invoke();
        assertEquals(HTTP_STATUS_NOCONTENT, response.getStatus());

        response.close();
    }

    @Test
    public void testDeleteScopeIsUnauthorizedForUnauthorizedUser() {
        // Arrange
        String scopeName = "scope-d";
        createScope(scopeName, "admin", "1111_aaaa");
        final String resourceURI = getURI() + "v1/scopes/" + scopeName;

        when(mockControllerService.deleteScope(scopeName)).thenReturn(
                CompletableFuture.completedFuture(
                        Controller.DeleteScopeStatus.newBuilder().setStatus(
                                Controller.DeleteScopeStatus.Status.SUCCESS).build()));

        Response response = this.invocationBuilder(resourceURI, "unauthorizedUser", "1111_aaaa")
                .buildDelete().invoke();
        assertEquals(HTTP_STATUS_UNAUTHORIZED, response.getStatus());

        response.close();
    }

    @Test
    public void testListStreamsReturnsEmptyListWhenUserHasNoStreamsAssigned() {
        // Arrange
        String resourceURI = getURI() + "v1/scopes/myscope/streams";

        Map<String, StreamConfiguration> streamsList = ImmutableMap.of("stream1", this.aStreamConfig(),
                                                                       "stream2", this.aStreamConfig());
        when(mockControllerService.listStreamsInScope("myscope")).thenReturn(CompletableFuture.completedFuture(streamsList));

        // Act
        Response response = this.invocationBuilder(resourceURI,
                "userAuthOnScopeButNotOnStreams", "1111_aaaa").buildGet().invoke();
        StreamsList listedStreams = response.readEntity(StreamsList.class);

        // Assert
        assertEquals(HTTP_STATUS_OK, response.getStatus());
        assertEquals(null, listedStreams.getStreams());

        response.close();
    }

    @Test
    public void testListStreamsReturnsAllStreamsWhenUserHasWildcardOnScope() {
        // Arrange
        String resourceURI = getURI() + "v1/scopes/myscope/streams";

        Map<String, StreamConfiguration> streamsList = ImmutableMap.of(
                "stream1", this.aStreamConfig(),
                "stream2", this.aStreamConfig(),
                "stream3", this.aStreamConfig());
        when(mockControllerService.listStreamsInScope("myscope")).thenReturn(CompletableFuture.completedFuture(streamsList));

        // Act
        Response response = this.invocationBuilder(resourceURI,
                "userAuthOnScopeAndReadOnAllStreams", "1111_aaaa").buildGet().invoke();
        StreamsList listedStreams = response.readEntity(StreamsList.class);

        // Assert
        assertEquals(HTTP_STATUS_OK, response.getStatus());
        assertEquals(3, listedStreams.getStreams().size());

        response.close();
    }

    @Test
    public void testListStreamsReturnsAllWhenUserHasWildCardAccess() {
        // Arrange
        String resourceURI = getURI() + "v1/scopes/myscope/streams";

        Map<String, StreamConfiguration> streamsList = ImmutableMap.of(
                "stream1", this.aStreamConfig(),
                "stream2", this.aStreamConfig(),
                "stream3", this.aStreamConfig());
        when(mockControllerService.listStreamsInScope("myscope")).thenReturn(CompletableFuture.completedFuture(streamsList));

        // Act
        Response response = this.invocationBuilder(resourceURI,
                "privilegedUser", "1111_aaaa").buildGet().invoke();
        StreamsList listedStreams = response.readEntity(StreamsList.class);

        // Assert
        assertEquals(HTTP_STATUS_OK, response.getStatus());
        assertEquals(3, listedStreams.getStreams().size());

        response.close();
    }

    @Test
    public void testUpdateStreamStateAuthorizedForPrivilegedUser() {
        String resourceURI = getURI() + "v1/scopes/myscope/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream("myscope", "stream1")).thenReturn(CompletableFuture.completedFuture(
                Controller.UpdateStreamStatus.newBuilder().setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = this.invocationBuilder(resourceURI, "privilegedUser", "1111_aaaa")
                .buildPut(Entity.json(streamState)).invoke();

        assertEquals("Update Stream State response code", HTTP_STATUS_OK, response.getStatus());
        response.close();
    }

    @Test
    public void testUpdateStreamStateAuthorizedForUserWithStreamWriteAccess() {
        String resourceURI = getURI() + "v1/scopes/myscope/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream("myscope", "stream1")).thenReturn(CompletableFuture.completedFuture(
                Controller.UpdateStreamStatus.newBuilder().setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = this.invocationBuilder(resourceURI, "userAuthOnScopeAndWriteOnSpecificStream", "1111_aaaa")
                .buildPut(Entity.json(streamState)).invoke();

        assertEquals("Update Stream State response code", HTTP_STATUS_OK, response.getStatus());
        response.close();
    }

    @Test
    public void testUpdateStreamStateAuthorizedWhenUserHasWildcardAccessOnScope() {

        String resourceURI = getURI() + "v1/scopes/myscope/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream("myscope", "stream1")).thenReturn(CompletableFuture.completedFuture(
                Controller.UpdateStreamStatus.newBuilder().setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = this.invocationBuilder(resourceURI,
                "userAuthOnScopeAndWriteOnAllStreams", "1111_aaaa")
                .buildPut(Entity.json(streamState)).invoke();

        assertEquals("Update Stream State response code", HTTP_STATUS_OK, response.getStatus());
        response.close();
    }

    @Test
    public void testUpdateStreamStateAuthorizedWhenUserHasReadOnlyAccessOnScopeChildren() {

        String resourceURI = getURI() + "v1/scopes/myscope/streams/stream1/state";

        // Test to seal a stream.
        when(mockControllerService.sealStream("myscope", "stream1")).thenReturn(CompletableFuture.completedFuture(
                Controller.UpdateStreamStatus.newBuilder().setStatus(Controller.UpdateStreamStatus.Status.SUCCESS).build()));
        StreamState streamState = new StreamState().streamState(StreamState.StreamStateEnum.SEALED);
        Response response = this.invocationBuilder(resourceURI,
                "userAuthOnScopeAndReadOnAllStreams", "1111_aaaa")
                .buildPut(Entity.json(streamState)).invoke();

        assertEquals("Update Stream State response code", HTTP_STATUS_FORBIDDEN, response.getStatus());
        response.close();
    }

    //region Private methods

    private Response createScope(String scopeName, String username, String password) {

        final String resourceURI = getURI() + "v1/scopes/";
        final CreateScopeRequest createScopeRequest = new CreateScopeRequest().scopeName(scopeName);

        // Test to create a new scope.
        when(mockControllerService.createScope(scopeName)).thenReturn(CompletableFuture.completedFuture(
                Controller.CreateScopeStatus.newBuilder().setStatus(
                        Controller.CreateScopeStatus.Status.SUCCESS).build()));
        return invocationBuilder(resourceURI, username, password).buildPost(Entity.json(createScopeRequest)).invoke();
    }

    private String getURI() {
        return "http://localhost:" + serverConfig.getPort() + "/";
    }

    private Invocation.Builder invocationBuilder(String resourceUri, String username, String password) {
        MultivaluedMap<String, Object> map = new MultivaluedHashMap<>();
        map.addAll(HttpHeaders.AUTHORIZATION, TestUtils.basicAuthToken(username, password));
        return client.target(resourceUri).request().headers(map);
    }

    private StreamConfiguration aStreamConfig() {
        return StreamConfiguration.builder()
                .scalingPolicy(ScalingPolicy.byEventRate(100, 2, 2))
                .retentionPolicy(RetentionPolicy.byTime(Duration.ofMillis(123L)))
                .build();
    }

    //endregion
}
