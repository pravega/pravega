/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.stat;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class AutoScalerConfigTest {

    // region Tests that verify the toString() method.

    // Note: It might seem odd that we are unit testing the toString() method of the code under test. The reason we are
    // doing that is that the method is hand-rolled and there is a bit of logic there that isn't entirely unlikely to fail.

    @Test
    public void testToStringIsSuccessfulWithAllNonDefaultConfigSpecified() {
        AutoScalerConfig config = AutoScalerConfig.builder()
                .with(AutoScalerConfig.TLS_CERT_FILE, "/cert.pem")
                .build();
        assertNotNull(config.toString());
    }

    @Test
    public void testToStringIsSuccessfulWithNoConfigSpecified() {
        AutoScalerConfig config = AutoScalerConfig.builder()
                .build();
        assertNotNull(config.toString());
    }

    // endregion
}
