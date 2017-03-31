/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

import org.junit.Assert;
import org.junit.Test;

public class NameVerifierTest {

    @Test
    public void testNameVerifier() {
        try {
            NameVerifier.validateName("stream");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameVerifier.validateName("stream/");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameVerifier.validateName(null);
            Assert.fail();
        } catch (NullPointerException e) {
            // expected
        }
    }
}
