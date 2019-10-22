/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import com.google.gson.Gson;

import java.util.Base64;

public class JwtTestUtils {

    static String createJwtBody(JwtBody jwt) {
        String json = new Gson().toJson(jwt);
        return Base64.getEncoder().encodeToString(json.getBytes());
    }

    static String dummyToken() {
        return String.format("header.%s.signature",
                createJwtBody(JwtBody.builder().expirationTime(Long.MAX_VALUE).build()));
    }
}
