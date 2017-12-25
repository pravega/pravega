/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.delegationtoken;

import io.pravega.client.auth.PravegaAuthHandler;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.*;

public class TokenVerifierImplTest {

    @Before
    public void setUp() throws Exception {

    }


    @Test
    public void testTokenVerifier() {
        AutoScalerConfig config = AutoScalerConfig.builder()
                                                  .with(AutoScalerConfig.AUTH_ENABLED, false)
                                                  .with(AutoScalerConfig.TOKEN_SIGNING_KEY, "secret")
                                                  .build();
        DelegationTokenVerifier tokenVerifier = new TokenVerifierImpl(config);

        //Auth disabled. No token is checked.
        tokenVerifier.verifyToken("xyz", null, PravegaAuthHandler.PravegaAccessControlEnum.READ);

        //Auth enabled, error on null token
        config = AutoScalerConfig.builder()
                                 .with(AutoScalerConfig.AUTH_ENABLED, true)
                                 .with(AutoScalerConfig.TOKEN_SIGNING_KEY, "secret")
                                 .build();
        tokenVerifier = new TokenVerifierImpl(config);
        DelegationTokenVerifier finalTokenVerifier = tokenVerifier;
        assertThrows(IllegalArgumentException.class, () -> {
            finalTokenVerifier.verifyToken("xyz", null, PravegaAuthHandler.PravegaAccessControlEnum.READ);
        });

        //TODO: Add more tests.. wildcard check, level mismatch, timer expiry.
    }

    @After
    public void tearDown() throws Exception {
    }
}