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

import com.google.common.base.Preconditions;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.pravega.common.Exceptions;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import javax.net.ssl.SSLException;

/**
 * A helper class containing utility methods for TLS functionality in Segment Store.
 */
@Slf4j
public class TLSHelper {

    public static final String TLS_HANDLER_NAME = "tls";

    /**
     * Creates a new instance of {@link SslContext}.
     *
     * @param pathToCertificateFile the path to the PEM-encoded server certificate file
     * @param pathToServerKeyFile the path to the PEM-encoded file containing the server's encrypted private key
     * @param tlsProtocolVersion the version of the TLS protocol
     * @return a {@link SslContext} built from the specified {@code pathToCertificateFile} and {@code pathToServerKeyFile}
     * @throws NullPointerException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is null
     * @throws IllegalArgumentException if either {@code pathToCertificateFile} or {@code pathToServerKeyFile} is empty
     * @throws RuntimeException if there is a failure in building the {@link SslContext}
     */
    public static SslContext newServerSslContext(String pathToCertificateFile, String pathToServerKeyFile, String[] tlsProtocolVersion) {
        Exceptions.checkNotNullOrEmpty(pathToCertificateFile, "pathToCertificateFile");
        Exceptions.checkNotNullOrEmpty(pathToServerKeyFile, "pathToServerKeyFile");
        Exceptions.checkArgument(tlsProtocolVersion != null, "tlsProtocolVersion", "Invalid TLS Protocol Version");
        return newServerSslContext(new File(pathToCertificateFile), new File(pathToServerKeyFile), tlsProtocolVersion);
    }

    /**
     * Creates a new instance of {@link SslContext}.
     *
     * @param certificateFile the PEM-encoded server certificate file
     * @param serverKeyFile the PEM-encoded file containing the server's encrypted private key
     * @param tlsProtocolVersion version of TLS protocol
     * @return a {@link SslContext} built from the specified {@code pathToCertificateFile} and {@code pathToServerKeyFile}
     * @throws NullPointerException if either {@code certificateFile} or {@code serverKeyFile} is null
     * @throws IllegalStateException if either {@code certificateFile} or {@code serverKeyFile} doesn't exist or is unreadable.
     * @throws RuntimeException if there is a failure in building the {@link SslContext}
     */
    public static SslContext newServerSslContext(File certificateFile, File serverKeyFile, String[] tlsProtocolVersion) {
        Preconditions.checkNotNull(certificateFile);
        Preconditions.checkNotNull(serverKeyFile);
        Preconditions.checkNotNull(tlsProtocolVersion);
        ensureExistAndAreReadable(certificateFile, serverKeyFile);

        try {
            SslContext result = SslContextBuilder.forServer(certificateFile, serverKeyFile)
                    .protocols(tlsProtocolVersion)
                    .build();
            log.debug("Done creating a new SSL Context for the server.");
            return result;
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void ensureExistAndAreReadable(File certificateFile, File serverKeyFile) {
        Preconditions.checkArgument(certificateFile.exists(), String.format("Certificate file %s doesn't exist",
                certificateFile.getAbsolutePath()));

        Preconditions.checkArgument(certificateFile.canRead(), "Certificate file %s can't be read",
                certificateFile.getAbsolutePath());

        Preconditions.checkArgument(serverKeyFile.exists(), String.format("Key file %s doesn't exist",
                serverKeyFile.getAbsolutePath()));

        Preconditions.checkArgument(serverKeyFile.canRead(), String.format("Key file %s can't be read",
                serverKeyFile.getAbsolutePath()));
    }
}
