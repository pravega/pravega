/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc.impl;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class GRPCServerConfigImplTest {

    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.

    @Test
    public void toStringReturnsSuccessfullyWithAllConfigSpecified() {
        GRPCServerConfigImpl config = new GRPCServerConfigImpl(9090, "localhost",
                9090, true, "/passwd", true,
                "/cert.pem", "./key.pem", "secret", 300, "/cert.pem",
                true, true);
        assertNotNull(config.toString());
    }

    @Test
    public void toStringReturnsSuccessfullyWithSomeConfigNullOrEmpty() {
        GRPCServerConfigImpl config = new GRPCServerConfigImpl(9090, null,
                9090, true, null, true,
                "", " ", "secret", 300, "/cert.pem",
                false, true);
        assertNotNull(config.toString());
    }

    @Test
    public void toStringReturnsSuccessfullyWithAccessTokenTtlNull() {
        GRPCServerConfigImpl config = new GRPCServerConfigImpl(9090, null,
                9090, true, null, true,
                "", " ", "secret", null, "/cert.pem",
                false, true);
        assertNotNull(config.toString());
    }

    // endregion
}
