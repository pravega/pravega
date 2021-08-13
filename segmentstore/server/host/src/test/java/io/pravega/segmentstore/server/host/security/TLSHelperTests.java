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

import static io.pravega.test.common.AssertExtensions.assertThrows;
import static org.junit.Assert.assertNotNull;

public class TLSHelperTests {

    private final static String PATH_EMPTY = "";
    private final static String PATH_NONEMPTY = "non-empty";
    private final static String PATH_NONEXISTENT = System.currentTimeMillis() + ".file";

    @Test
    public void testNewServerSslContextSucceedsWhenInputIsValid() {
        String pathToCertificateFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME;
        String pathToKeyFile = "../../../config/" + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME;

        SslContext sslCtx = TLSHelper.newServerSslContext(new File(pathToCertificateFile),
                new File(pathToKeyFile), SecurityConfigDefaults.TLS_PROTOCOL_VERSION);

        assertNotNull(sslCtx);
    }

    @Test
    public void testNewServerSslContextFailsWhenInputIsNull() {
        assertThrows("Null pathToCertificateFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(null, PATH_NONEMPTY, SecurityConfigDefaults.TLS_PROTOCOL_VERSION),
                e -> e instanceof NullPointerException);

        assertThrows("Null pathToServerKeyFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEMPTY, null, SecurityConfigDefaults.TLS_PROTOCOL_VERSION),
                e -> e instanceof NullPointerException);
    }

    @Test
    public void testNewServerSslContextFailsWhenInputIsEmpty() {
        assertThrows("Empty pathToCertificateFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_EMPTY, PATH_NONEMPTY, SecurityConfigDefaults.TLS_PROTOCOL_VERSION),
                e -> e instanceof IllegalArgumentException);

        assertThrows("Empty pathToServerKeyFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEMPTY, PATH_EMPTY, SecurityConfigDefaults.TLS_PROTOCOL_VERSION),
                e -> e instanceof IllegalArgumentException);
    }

    @Test
    public void testNewServerSslContextFailsWhenInputFilesDontExist() {
        assertThrows("Non-existent pathToCertificateFile wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEXISTENT, PATH_NONEMPTY, SecurityConfigDefaults.TLS_PROTOCOL_VERSION),
                e -> e instanceof IllegalArgumentException);

        assertThrows("Non-existent pathToServerKeyFile argument wasn't rejected.",
                () -> TLSHelper.newServerSslContext(PATH_NONEMPTY, PATH_NONEXISTENT, SecurityConfigDefaults.TLS_PROTOCOL_VERSION),
                e -> e instanceof IllegalArgumentException);
    }
}
