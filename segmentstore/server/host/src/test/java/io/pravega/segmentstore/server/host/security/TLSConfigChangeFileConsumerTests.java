/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host.security;

import io.netty.handler.ssl.SslContext;
import io.pravega.test.common.SecurityConfigDefaults;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class TLSConfigChangeFileConsumerTests {

    @Test(expected = NullPointerException.class)
    public void testNullCtorArgumentsAreRejected() {
        new TLSConfigChangeFileConsumer(new AtomicReference<>(null), null, null, null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyPathToCertificateFileIsRejected() {
        TLSConfigChangeFileConsumer subjectUnderTest = new TLSConfigChangeFileConsumer(new AtomicReference<>(null),
                    "", "non-existent", SecurityConfigDefaults.TLS_PROTOCOL_VERSION);
        subjectUnderTest.accept(null);

        assertEquals(1, subjectUnderTest.getNumOfConfigChangesSinceStart());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testEmptyPathToKeyFileIsRejected() {
        TLSConfigChangeFileConsumer subjectUnderTest = new TLSConfigChangeFileConsumer(new AtomicReference<>(null),
                "non-existent", "", SecurityConfigDefaults.TLS_PROTOCOL_VERSION);
        subjectUnderTest.accept(null);
        assertEquals(1, subjectUnderTest.getNumOfConfigChangesSinceStart());
    }

    @Test
    public void testInvocationIncrementsReloadCounter() {
        String pathToCertificateFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;

        AtomicReference<SslContext> sslCtx = new AtomicReference<>(TLSHelper.newServerSslContext(
                new File(pathToCertificateFile), new File(pathToKeyFile), SecurityConfigDefaults.TLS_PROTOCOL_VERSION));

        TLSConfigChangeFileConsumer subjectUnderTest = new TLSConfigChangeFileConsumer(sslCtx, pathToCertificateFile,
                pathToKeyFile, SecurityConfigDefaults.TLS_PROTOCOL_VERSION);
        subjectUnderTest.accept(null);

        assertEquals(1, subjectUnderTest.getNumOfConfigChangesSinceStart());

        subjectUnderTest.accept(null);
        assertEquals(2, subjectUnderTest.getNumOfConfigChangesSinceStart());
    }
}
