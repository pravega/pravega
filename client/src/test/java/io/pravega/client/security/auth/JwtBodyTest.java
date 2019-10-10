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
import org.junit.Test;
import java.time.Instant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JwtBodyTest {

    @Test
    public void testSerialize() {
        JwtBody jwtBody = JwtBody.builder()
                .subject("subject")
                .audience("segmentstore")
                .issuedAtTime(Instant.now().getEpochSecond())
                .expirationTime(Instant.now().plusSeconds(50).getEpochSecond())
                .build();

        String json = new Gson().toJson(jwtBody);
        assertNotNull(json);
    }

    @Test
    public void testDeserialize() {
        String json = "{\"sub\":\"subject\",\"aud\":\"segmentstore\",\"iat\":1569837384,\"exp\":1569837434}";
        JwtBody jwtBody = new Gson().fromJson(json, JwtBody.class);

        assertEquals("subject", jwtBody.getSubject());
        assertEquals("segmentstore", jwtBody.getAudience());
        assertEquals(new Long(1569837384), jwtBody.getIssuedAtTime());
        assertEquals(new Long(1569837434), jwtBody.getExpirationTime());
    }
}
