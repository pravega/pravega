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

import io.pravega.test.common.TestUtils;

import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

public class UserSecureStreamMetaDataTests extends SecureStreamMetaDataTests {

    @Override
    protected Invocation.Builder addAuthHeaders(Invocation.Builder request) {
        MultivaluedMap<String, Object> map = new MultivaluedHashMap<>();
        map.addAll(HttpHeaders.AUTHORIZATION, TestUtils.basicAuthToken("user1", "1111_aaaa"));
        return request.headers(map);
    }

}
