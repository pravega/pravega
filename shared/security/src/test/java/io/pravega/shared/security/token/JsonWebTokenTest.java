/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.security.token;


import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

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
                        null, -2)
        );
    }
}
