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
package io.pravega.controller.server.rpc.grpc.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.auth.AuthPluginConfig;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

import io.pravega.controller.util.Config;
import lombok.Builder;
import lombok.Data;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfigImpl implements GRPCServerConfig {
    private final int port;
    private final Optional<String> publishedRPCHost;
    private final Optional<Integer> publishedRPCPort;
    private final boolean authorizationEnabled;
    private final String userPasswordFile;
    private final boolean tlsEnabled;
    private final String[] tlsProtocolVersion;
    private final String tlsCertFile;
    private final String tlsKeyFile;
    private final String tokenSigningKey;
    private final Integer accessTokenTTLInSeconds;
    private final boolean isRGWritesWithReadPermEnabled;
    private final String tlsTrustStore;
    private final boolean replyWithStackTraceOnError;
    private final boolean requestTracingEnabled;

    @Builder
    public GRPCServerConfigImpl(final int port, final String publishedRPCHost, final Integer publishedRPCPort,
                                boolean authorizationEnabled, String userPasswordFile, boolean tlsEnabled, String[] tlsProtocolVersion,
                                String tlsCertFile, String tlsKeyFile, String tokenSigningKey,
                                Integer accessTokenTTLInSeconds, boolean isRGWritesWithReadPermEnabled,
                                String tlsTrustStore,
                                boolean replyWithStackTraceOnError, boolean requestTracingEnabled) {

        Preconditions.checkArgument(port > 0, "Invalid port.");
        if (publishedRPCHost != null) {
            Exceptions.checkNotNullOrEmpty(publishedRPCHost, "publishedRPCHost");
        }
        if (publishedRPCPort != null) {
            Preconditions.checkArgument(publishedRPCPort > 0, "publishedRPCPort should be a positive integer");
        }
        if (accessTokenTTLInSeconds != null) {
            Preconditions.checkArgument(accessTokenTTLInSeconds == -1 || accessTokenTTLInSeconds >= 0,
                    "accessTokenTtlInSeconds should be -1 (token never expires), 0 (token immediately expires) "
                            + "or a positive integer representing the number of seconds after which the token expires.");
        }

        this.port = port;
        this.publishedRPCHost = Optional.ofNullable(publishedRPCHost);
        this.publishedRPCPort = Optional.ofNullable(publishedRPCPort);
        this.authorizationEnabled = authorizationEnabled;
        this.userPasswordFile = userPasswordFile;
        this.tlsEnabled = tlsEnabled;
        if (tlsProtocolVersion == null) {
            this.tlsProtocolVersion = Config.TLS_PROTOCOL_VERSION.toArray(new String[Config.TLS_PROTOCOL_VERSION.size()]);
        } else {
            this.tlsProtocolVersion =  Arrays.copyOf(tlsProtocolVersion, tlsProtocolVersion.length);
        }
        this.tlsCertFile = tlsCertFile;
        this.tlsKeyFile = tlsKeyFile;
        this.tlsTrustStore = tlsTrustStore;
        this.tokenSigningKey = tokenSigningKey;
        this.accessTokenTTLInSeconds = accessTokenTTLInSeconds;
        this.isRGWritesWithReadPermEnabled = isRGWritesWithReadPermEnabled;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
        this.requestTracingEnabled = requestTracingEnabled;
    }

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder("GRPCServerConfigImpl(")

                // Endpoint config
                .append(String.format("port: %d, ", port))
                .append(String.format("publishedRPCHost: %s, ",
                        publishedRPCHost.isPresent() ? publishedRPCHost.get() : "null"))
                .append(String.format("publishedRPCPort: %s, ",
                        publishedRPCPort.isPresent() ? publishedRPCPort.get() : "null"))

                // Auth config
                .append(String.format("authorizationEnabled: %b, ", authorizationEnabled))
                .append(String.format("userPasswordFile is %s, ",
                        Strings.isNullOrEmpty(userPasswordFile) ? "unspecified" : "specified"))
                .append(String.format("tokenSigningKey is %s, ",
                        Strings.isNullOrEmpty(tokenSigningKey) ? "unspecified" : "specified"))
                .append(String.format("accessTokenTTLInSeconds: %s, ", accessTokenTTLInSeconds))
                .append(String.format("isRGWritesWithReadPermEnabled: %b, ", isRGWritesWithReadPermEnabled))

                // TLS config
                .append(String.format("tlsEnabled: %b, ", tlsEnabled))
                .append(String.format("tlsProtocolVersion: %s, ", Arrays.toString(tlsProtocolVersion)))
                .append(String.format("tlsCertFile is %s, ",
                        Strings.isNullOrEmpty(tlsCertFile) ? "unspecified" : "specified"))
                .append(String.format("tlsKeyFile is %s, ",
                        Strings.isNullOrEmpty(tlsKeyFile) ? "unspecified" : "specified"))
                .append(String.format("tlsTrustStore is %s, ",
                        Strings.isNullOrEmpty(tlsTrustStore) ? "unspecified" : "specified"))

                // Request processing config
                .append(String.format("replyWithStackTraceOnError: %b, ", replyWithStackTraceOnError))
                .append(String.format("requestTracingEnabled: %b", requestTracingEnabled))

                .append(")")
                .toString();
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
