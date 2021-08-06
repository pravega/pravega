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
package io.pravega.shared.rest.impl;

import com.google.common.base.Strings;
import io.pravega.auth.AuthPluginConfig;
import io.pravega.common.Exceptions;
import io.pravega.shared.rest.RESTServerConfig;
import lombok.Builder;
import lombok.Getter;

import java.util.Arrays;
import java.util.Properties;

/**
 * REST server config.
 */
@Getter
@Builder
public class RESTServerConfigImpl implements RESTServerConfig {
    private final String host;
    private final int port;
    private final boolean authorizationEnabled;
    private final String userPasswordFile;
    private final boolean tlsEnabled;
    private final String[] tlsProtocolVersion;
    private final String keyFilePath;
    private final String keyFilePasswordPath;

    public static final class RESTServerConfigImplBuilder {
        private String[] tlsProtocolVersion = new String[] {"TLSv1.2", "TLSv1.3"};

        public RESTServerConfigImpl build() {
            return new RESTServerConfigImpl(host, port, authorizationEnabled, userPasswordFile, tlsEnabled, tlsProtocolVersion, keyFilePath, keyFilePasswordPath);
        }
    }

    RESTServerConfigImpl(final String host, final int port, boolean authorizationEnabled, String userPasswordFile,
                         boolean tlsEnabled, String[] tlsProtocolVersion, String keyFilePath, String keyFilePasswordPath) {
        Exceptions.checkNotNullOrEmpty(host, "host");
        Exceptions.checkArgument(port > 0, "port", "Should be positive integer");
        Exceptions.checkArgument(!tlsEnabled || !Strings.isNullOrEmpty(keyFilePath),
                "TLS", "KeyFilePath should not be empty when TLS is enabled. ");

        this.host = host;
        this.port = port;
        this.tlsEnabled = tlsEnabled;
        this.tlsProtocolVersion = Arrays.copyOf(tlsProtocolVersion, tlsProtocolVersion.length);
        this.keyFilePath = keyFilePath;
        this.keyFilePasswordPath = keyFilePasswordPath;
        this.authorizationEnabled = authorizationEnabled;
        this.userPasswordFile = userPasswordFile;
    }

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("host: %s, ", host))
                .append(String.format("port: %d, ", port))
                .append(String.format("tlsEnabled: %b, ", tlsEnabled))
                .append(String.format("tlsProtocolVersion: %s, ", Arrays.toString(tlsProtocolVersion)))
                .append(String.format("keyFilePath is %s, ",
                        Strings.isNullOrEmpty(keyFilePath) ? "unspecified" : "specified"))
                .append(String.format("keyFilePasswordPath is %s",
                        Strings.isNullOrEmpty(keyFilePasswordPath) ? "unspecified" : "specified"))
                .append(String.format("authorizationEnabled: %b, ", authorizationEnabled))
                .append(String.format("userPasswordFile is %s, ",
                        Strings.isNullOrEmpty(userPasswordFile) ? "unspecified" : "specified"))
                .append(")")
                .toString();
    }

    @Override
    public boolean isAuthorizationEnabled() {
        return this.authorizationEnabled;
    }

    @Override
    public boolean isTlsEnabled() {
        return this.tlsEnabled;
    }

    @Override
    public String[] tlsProtocolVersion() {
        return Arrays.copyOf(this.tlsProtocolVersion, this.tlsProtocolVersion.length);
    }

    @Override
    public Properties toAuthHandlerProperties() {
        Properties props = new Properties();
        if (this.userPasswordFile != null) {
            props.setProperty(AuthPluginConfig.BASIC_AUTHPLUGIN_DATABASE, this.userPasswordFile);
        }
        return props;
    }
}
