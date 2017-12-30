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
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.test.common.TestUtils;
import javax.ws.rs.client.Invocation;
import org.junit.Before;

public class SecureStreamMetaDataTests extends  StreamMetaDataTests {
    @Override
    @Before
    public void setup() {
        this.authManager = new PravegaAuthManager(GRPCServerConfigImpl.builder()
                                                                      .authorizationEnabled(true)
                                                                      .tlsCertFile("../config/cert.pem")
                                                                      .tlsKeyFile("../config/key.pem")
                                                                      .userPasswdFile("../config/passwd")
                                                                      .port(1000)
                                                                      .build());
        ServerBuilder<?> server = ServerBuilder.forPort(TestUtils.getAvailableListenPort());
        this.authManager.registerInterceptors(server);
        super.setup();
    }

    @Override
    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        return request.header("method", "testHandler");
    }
}
