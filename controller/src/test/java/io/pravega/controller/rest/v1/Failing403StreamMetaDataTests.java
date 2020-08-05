/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.rest.v1;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import io.pravega.controller.server.security.auth.handler.TestAuthHandler;
import org.junit.Before;

public class Failing403StreamMetaDataTests extends  FailingSecureStreamMetaDataTests {
    @Override
    @Before
    public void setup() throws Exception {
        expectedResult = 403;
        super.setup();
    }

    @Override
    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        MultivaluedMap<String, Object> map = new MultivaluedHashMap<>();
        map.addAll(HttpHeaders.AUTHORIZATION, TestAuthHandler.testAuthToken(TestAuthHandler.DUMMY_USER));
        return request.headers(map);
    }
}
