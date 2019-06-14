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
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class JsonWebTokenTest {

    @Test
    public void testConstructionIsSuccessfulWithMinimalValidInput() {
        String token = new JsonWebToken("subject", "audience", "secret".getBytes()).toCompactString();
        assertNotNull(token);
        assertNotEquals("", token);
    }

    @Test
    public void testInputIsRejectedWhenMandatoryInputIsNull() {
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
    public void testCtorRejectsInvalidTtl() {
        assertThrows(IllegalArgumentException.class,
                () -> new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
                        -2, null)
        );
    }

    @Test
    public void testPopulatesTokenWithAllInputs() throws TokenException {
        Map<String, Object> customClaims = new HashMap<>();
        //customClaims.put("abc", "READ_UPDATE");

        String token =  new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
                                        Integer.MAX_VALUE, customClaims)
                            .toCompactString();

        Claims allClaims = JsonWebToken.parseClaims(token, "signingKeyString".getBytes());

        assertEquals("subject", allClaims.getSubject());
        assertEquals("audience", allClaims.getAudience());
   }

   @Test
   public void testTokenDoesNotContainExpiryWhenTtlIsMinusOne() throws TokenException {
       String token =  new JsonWebToken("subject", "audience", "signingKeyString".getBytes(),
               -1, null)
               .toCompactString();

       Claims allClaims = JsonWebToken.parseClaims(token, "signingKeyString".getBytes());
       assertNull(allClaims.getExpiration());
   }
}
