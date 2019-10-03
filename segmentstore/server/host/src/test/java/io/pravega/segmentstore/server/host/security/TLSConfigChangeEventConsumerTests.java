/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.security;

import io.netty.handler.ssl.SslContext;
import io.pravega.test.common.SecurityConfigDefaults;
import org.junit.Test;
import java.io.File;
import java.nio.file.WatchEvent;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class TLSConfigChangeEventConsumerTests {

    @Test (expected = NullPointerException.class)
    public void testNullCtorArgumentsAreRejected() {
        new TLSConfigChangeEventConsumer(new AtomicReference<>(null), null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyPathToCertificateFileIsRejected() {
        TLSConfigChangeEventConsumer subjectUnderTest = new TLSConfigChangeEventConsumer(new AtomicReference<>(null),
                "", "non-existent");
        subjectUnderTest.accept(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyPathToKeyFileIsRejected() {
        TLSConfigChangeEventConsumer subjectUnderTest = new TLSConfigChangeEventConsumer(new AtomicReference<>(null),
                "non-existent", "");
        subjectUnderTest.accept(null);
    }

    @Test
    public void testInvocationIncrementsReloadCounter() {
        String pathToCertificateFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;

        AtomicReference<SslContext> sslCtx = new AtomicReference<>(TLSHelper.newServerSslContext(
                new File(pathToCertificateFile), new File(pathToKeyFile)));

        TLSConfigChangeEventConsumer subjectUnderTest = new TLSConfigChangeEventConsumer(sslCtx, pathToCertificateFile,
                pathToKeyFile);
        subjectUnderTest.accept(null);

        assertEquals(1, subjectUnderTest.getNumOfConfigChangesSinceStart());

        subjectUnderTest.accept(mock(WatchEvent.class));
        assertEquals(2, subjectUnderTest.getNumOfConfigChangesSinceStart());
    }
}
