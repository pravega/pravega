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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DelegationTokenProxyImplTest {

    @Test
    public void testDefaultCtorReturnsEmptyToken() {
        assertEquals("", new DelegationTokenProxyImpl().retrieveToken());
    }

    @Test
    public void testDefaultCtorReturnsEmptyTokenOnRefresh() {
        assertEquals("", new DelegationTokenProxyImpl().refreshToken());
    }




    @Test
    public void testReturnsExistingTokenIfExpiryIsNotSet() {
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature
        DelegationTokenProxyImpl proxy = new DelegationTokenProxyImpl(token, new FakeController(),
                "testscope", "teststream");
        assertEquals(token, proxy.retrieveToken());
    }

    /*@Test
    public void testReturnsExistingTokenIfNotNearingExpiry() {
        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "jdoe",
        //        "aud": "segmentstore",
        //         "iat": 1569324678,
        //         "exp": 2147483647
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzUxMiJ9", // header
                "eyJzdWIiOiJqZG9lIiwiYXVkIjoic2VnbWVudHN0b3JlIiwiaWF0IjoxNTY5MzI0Njc4LCJleHAiOjIxNDc0ODM2NDd9", // body
                "7fcgsw5T2VThK48mLG_z1QCxiYCHlGdGao2LprF9cs4-5xd7mIRGuX6sQnYgwA1pB47X-5ShGeU3HKyELkrMiA"); // signature
    }*/
}

