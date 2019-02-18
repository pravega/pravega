/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import io.grpc.ServerBuilder;
import io.pravega.controller.server.rpc.auth.PravegaAuthManager;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.test.common.TestUtils;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.Before;

import java.io.File;
import java.io.FileWriter;

public class SecureStreamMetaDataTests extends  StreamMetaDataTests {
    @Override
    @Before
    public void setup() throws Exception {
        File file = File.createTempFile("passwd", ".txt");

        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();

        try (FileWriter writer = new FileWriter(file.getAbsolutePath())) {
            String passwd = passwordEncryptor.encryptPassword("1111_aaaa");

            // Admin has READ_WRITE permission to everything
            writer.write("admin:" + passwd + ":*,READ_UPDATE\n");

            // User "user1" can:
            //    - list, create and delete scopes
            //    - Create and delete streams within scopes "scope1" and "scope2". Also if "user1" lists scopes,
            //      she'll see those scopes, but not "scope3".
            writer.write("user1:" + passwd + ":/,READ_UPDATE;scope1,READ_UPDATE;scope2,READ_UPDATE;\n");

            writer.write("user2:" + passwd + ":/,READ;scope3,READ_UPDATE;\n");
            writer.close();
        }

        this.authManager = new PravegaAuthManager(GRPCServerConfigImpl.builder()
                                                                      .authorizationEnabled(true)
                                                                      .tlsCertFile("../config/cert.pem")
                                                                      .tlsKeyFile("../config/key.pem")
                                                                      .userPasswordFile(file.getAbsolutePath())
                                                                      .port(1000)
                                                                      .build());
        ServerBuilder<?> server = ServerBuilder.forPort(TestUtils.getAvailableListenPort());
        this.authManager.registerInterceptors(server);
        super.setup();
    }

    @Override
    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        MultivaluedMap<String, Object> map = new MultivaluedHashMap<>();
        map.addAll(HttpHeaders.AUTHORIZATION, TestUtils.basicAuthToken("admin", "1111_aaaa"));
        return request.headers(map);
    }
}
