/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import io.grpc.ServerBuilder;
import io.pravega.auth.AuthHandler;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.client.stream.impl.Credentials;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.common.auth.AuthConstants;
import io.pravega.common.auth.AuthenticationException;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.io.FileWriter;
import java.net.URI;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PravegaAuthManagerTest {

    private final ControllerServiceGrpc.ControllerServiceImplBase serviceImpl = new ControllerServiceGrpc.ControllerServiceImplBase() {
        @Override
        public void createScope(io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo request,
                                io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus> responseObserver) {
            responseObserver.onNext(Controller.CreateScopeStatus.newBuilder().build());
        }
    };

    private File file;

    @Before
    public void setUp() throws Exception {
        file = File.createTempFile("passwd", ".txt");
        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

        try (FileWriter writer = new FileWriter(file.getAbsolutePath())) {
            writer.write("#:\n");
            writer.write(":\n");
            writer.write("::\n");
            writer.write(":::\n");
            writer.write("dummy:password:\n");
            writer.write("dummy1:password:readresource;;\n");
            writer.write("dummy2:password:readresource;specificresouce,READ;totalaccess,READ_UPDATE\n");
            writer.write("dummy3:" + passwordEncryptor.encryptPassword("password") + ":readresource;specificresouce,READ;totalaccess,READ_UPDATE\n");
            writer.write("dummy4:" + passwordEncryptor.encryptPassword("password") + ":readresource;specificresouce,READ;*,READ_UPDATE\n");
            writer.close();
        }

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void registerInterceptors() throws Exception {
        //Test the registration method.
        GRPCServerConfig config = GRPCServerConfigImpl.builder()
                                                      .authorizationEnabled(true)
                                                      .userPasswordFile(file.getAbsolutePath())
                                                      .port(1000)
                                                      .build();

        PravegaAuthManager manager = new PravegaAuthManager(config);
        int port = TestUtils.getAvailableListenPort();
        ServerBuilder<?> server = ServerBuilder.forPort(port).useTransportSecurity(new File("../config/cert.pem"),
                new File("../config/key.pem"));

        server.addService(serviceImpl);
        manager.registerInterceptors(server);
        server.build().start();

        InlineExecutor executor = new InlineExecutor();
        @Cleanup
        final ControllerImpl controllerClient = new ControllerImpl(ControllerImplConfig.builder()
                .clientConfig(ClientConfig.builder()
                                          .controllerURI(URI.create("tcp://localhost:" + port)).build())
                .retryAttempts(1).build(),
                executor);

        //Malformed authorization header.
        assertThrows(AuthenticationException.class, () ->
                manager.authenticate("hi", "", AuthHandler.Permissions.READ));

        //Non existent interceptor method.
        assertThrows(AuthenticationException.class, () ->
        manager.authenticate("hi", credentials("invalid", ""), AuthHandler.Permissions.READ));

        //Specify a valid method but malformed parameters for password interceptor.
        assertThrows(AuthenticationException.class, () ->
        manager.authenticate("hi", credentials(AuthConstants.BASIC, ""), AuthHandler.Permissions.READ));

        //Specify a valid method but incorrect password for password interceptor.
        assertThrows(AuthenticationException.class, () ->
                manager.authenticate("hi", basic("dummy3", "wrong"), AuthHandler.Permissions.READ));

        //Specify a valid method and parameters but invalid resource for default interceptor.
        assertFalse("Not existent resource should return false",
                manager.authenticate("invalid", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        //Valid parameters for default interceptor
        assertTrue("Read access for read resource should return true",
                manager.authenticate("readresource", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        //Stream/scope access should be extended to segment.
        assertTrue("Read access for read resource should return true",
                manager.authenticate("readresource/segment", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        //Levels of access
        assertFalse("Write access for read resource should return false",
                manager.authenticate("readresource", basic("dummy3", "password"), AuthHandler.Permissions.READ_UPDATE));

        assertTrue("Read access for write resource should return true",
                manager.authenticate("totalaccess", basic("dummy3", "password"), AuthHandler.Permissions.READ));

        assertTrue("Write access for write resource should return true",
                manager.authenticate("totalaccess", basic("dummy3", "password"), AuthHandler.Permissions.READ_UPDATE));

        //Check the wildcard access
        assertTrue("Write access for write resource should return true",
                manager.authenticate("totalaccess", basic("dummy4", "password"), AuthHandler.Permissions.READ_UPDATE));

        assertTrue("Test handler should be called", manager.authenticate("any", testHandler(), AuthHandler.Permissions.READ));

        assertThrows(RetriesExhaustedException.class, () -> controllerClient.createScope("hi").join());
    }

    private static String credentials(String scheme, String token) {
        return scheme + " " + token;
    }

    private static String credentials(Credentials credentials) {
        return credentials(credentials.getAuthenticationType(), credentials.getAuthenticationToken());
    }

    private static String basic(String userName, String password) {
        return credentials(new DefaultCredentials(password, userName));
    }

    private static String testHandler() {
        return credentials("testHandler", "token");
    }
}