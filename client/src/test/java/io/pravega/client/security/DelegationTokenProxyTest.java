/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DelegationTokenProxyTest {

    @Test
    public void testExpirationTimeIsNullIfDelegationTokenIsEmpty() {
        assertNull("Expiration time is not null", new DelegationTokenProxy().extractExpirationTime(""));
    }

    @Test
    public void testExpirationTimeIsNullIfExpInBodyIsNotSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "1234567890",
        //        "name": "John Doe",
        //        "iat": 1516239022
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9", // header
                "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ", // body
                "SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"); // signature

        assertNull("Expiration time is not null", new DelegationTokenProxy().extractExpirationTime(token));
    }

    @Test
    public void testExpirationTimeIsNotNullIfExpInBodyIsSet() {

        // See decoded parts at https://jwt.io/.
        //
        // The body decodes to:
        //     {
        //        "sub": "1234567890",
        //        "name": "John Doe",
        //        "iat": 1516239022
        //     }
        String token = String.format("%s.%s.%s",
                "eyJhbGciOiJIUzUxMiJ9", // header
                "eyJzdWIiOiJzdWJqZWN0IiwiYXVkIjoiYXVkaWVuY2UiLCJpYXQiOjE1NjkzMjQ2NzgsImV4cCI6MTU2OTMyNDY4M30", // body
                "dbyfb1FfeuBydFNydh4AuHSByoxb5s1TxRgvs40VIXnNCJ6870ibAOVZ83AAiJ_bpoQpD71wtmqFY4gx_NzfLA"); // signature

        assertNotNull("Expiration time is null", new DelegationTokenProxy().extractExpirationTime(token));

    }
}
