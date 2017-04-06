/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.util;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.util.concurrent.TimeUnit;

public class NameUtilsTest {

    //Ensure each test completes within 10 seconds.
    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Test
    public void testUserStreamNameVerifier() {
        try {
            NameUtils.validateUserStreamName("stream123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateUserStreamName("_stream");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateUserStreamName(null);
            Assert.fail();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testStreamNameVerifier() {
        try {
            NameUtils.validateStreamName("_systemstream123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateStreamName("stream123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateStreamName("system_stream123");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateStreamName("stream/123");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateStreamName(null);
            Assert.fail();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testUserScopeNameVerifier() {
        try {
            NameUtils.validateUserScopeName("stream123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateUserScopeName("_stream");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateUserScopeName(null);
            Assert.fail();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testScopeNameVerifier() {
        try {
            NameUtils.validateScopeName("_systemscope123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateScopeName("userscope123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateScopeName("system_scope");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateScopeName("system/scope");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateScopeName(null);
            Assert.fail();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testReaderGroupNameVerifier() {
        try {
            NameUtils.validateReaderGroupName("stream123");
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            NameUtils.validateReaderGroupName("_stream");
            Assert.fail();
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            NameUtils.validateReaderGroupName(null);
            Assert.fail();
        } catch (NullPointerException e) {
            // expected
        }
    }

    @Test
    public void testInternalStreamName() {
        Assert.assertTrue(NameUtils.getInternalNameForStream("stream").startsWith(
                NameUtils.INTERNAL_NAME_PREFIX));
    }
}
