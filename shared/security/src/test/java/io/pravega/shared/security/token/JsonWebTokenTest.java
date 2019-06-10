/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.security.token;

import io.jsonwebtoken.Claims;
import io.pravega.auth.TokenException;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static io.pravega.test.common.AssertExtensions.assertThrows;

public class JsonWebTokenTest {

    @Test
    public void testConstructionIsSuccessfullWithMinimalValidInput() {
        String token = new JsonWebToken("subject", "audience", "secret".getBytes()).toCompactString();
        assertEquals("eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiJzdWJqZWN0IiwiYXVkIjoiYXVkaWVuY2UifQ.i2DZBbiMIvEcdmKcrB9oMjTXORRONJLiKEf0h9ies22j3NYwMbZwgWLGdhCbcIahDp4j4dW2YRXu9QVugYKaVw",
                token);
    }

    @Test
    public void testConstructionFailsWhenMandatoryInputIsNull() {
        assertThrows(NullPointerException.class,
                () -> new JsonWebToken(null, "audience", "signingKeyString".getBytes())
        );

        assertThrows(NullPointerException.class,
                () -> new JsonWebToken("subject", null, "signingKeyString".getBytes())
        );

        assertThrows(NullPointerException.class,
                () -> new JsonWebToken("subject", "audience", null)
        );
    }

    @Test
    public void testpopulatesTokenWithAllInputs() throws TokenException {
        Map<String, Object> customClaims = new HashMap<>();
        //customClaims.put("abc", "READ_UPDATE");

        String token =  new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
                                        Integer.MAX_VALUE, customClaims)
                            .toCompactString();

        Claims allClaims = JsonWebToken.parseClaims(token, "signingKeyString".getBytes());

        assertEquals("subject", allClaims.getSubject());
        assertEquals("audience", allClaims.getAudience());
   }
}
