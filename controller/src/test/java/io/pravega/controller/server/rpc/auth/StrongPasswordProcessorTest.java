/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import org.junit.Assert;
import org.junit.Test;

public class StrongPasswordProcessorTest {

    @Test
    public void checkPassword() throws InvalidKeySpecException, NoSuchAlgorithmException {
        StrongPasswordProcessor processor = StrongPasswordProcessor.builder().iterations(5000).build();
        String encrypted = processor.encryptPassword("1111_aaaa");
        StrongPasswordProcessor secondProcessor = StrongPasswordProcessor.builder().iterations(5000).build();

        secondProcessor.checkPassword("1111_aaaa", encrypted);

        StrongPasswordProcessor failingProcessor = StrongPasswordProcessor.builder().iterations(1000).build();
        Assert.assertTrue("Passwords with different iterations should not match",
                !encrypted.equals(failingProcessor.encryptPassword("1111_aaaa")));
    }
}