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
package io.pravega.cli.admin.utils;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

/**
 * Configuration for CLI client.
 */
public final class CLIConfig {

    public enum MetadataBackends {
        SEGMENTSTORE, ZOOKEEPER
    }

    private static final String PROTOCOL_PATTERN = "://";

    private static final Property<String> CONTROLLER_REST_URI = Property.named("controller.connect.rest.uri", "localhost:9091");
    private static final Property<String> CONTROLLER_GRPC_URI = Property.named("controller.connect.grpc.uri", "localhost:9090");
    private static final Property<Boolean> AUTH_ENABLED = Property.named("channel.auth", true);
    private static final Property<String> USER_NAME = Property.named("credentials.username", "");
    private static final Property<String> PASSWORD = Property.named("credentials.pwd", "");
    private static final Property<Boolean> TLS_ENABLED = Property.named("channel.tls", false);
    private static final Property<String> TRUSTSTORE_JKS = Property.named("trustStore.location", "");
    private static final Property<Integer> TRUSTSTORE_ACCESS_TOKEN_TTL_SECONDS = Property.named("trustStore.access.token.ttl.seconds", 10);
    private static final Property<String> METADATA_BACKEND = Property.named("store.metadata.backend", MetadataBackends.SEGMENTSTORE.name());

    private static final String COMPONENT_CODE = "cli";

    /**
     * The Controller REST URI. Recall to set "http" or "https" depending on the TLS configuration of the CLI.
     */
    @Getter
    private final String controllerRestURI;

    /**
     * The Controller GRPC URI. Recall to set "tcp" or "tls" depending on the TLS configuration of the CLI.
     */
    @Getter
    private final String controllerGrpcURI;

    /**
     * Defines whether or not to use authentication in CLI requests.
     */
    @Getter
    private final boolean authEnabled;

    /**
     * Defines whether or not to use tls in Controller requests.
     */
    @Getter
    private final boolean tlsEnabled;

    /**
     * User name if authentication is configured in the CLI.
     */
    @Getter
    private final String userName;

    /**
     * Password if authentication is configured in the CLI.
     */
    @Getter
    private final String password;

    /**
     * Truststore if TLS is configured in the CLI.
     */
    @Getter
    private final String truststore;

    /**
     * Truststore access token ttl if TLS is configured in the CLI.
     */
    @Getter
    private final Duration accessTokenTtl;

    /**
     * Controller metadata backend. At the moment, its values can only be "segmentstore" or "zookeeper".
     */
    @Getter
    private final String metadataBackend;

    private CLIConfig(TypedProperties properties) throws ConfigurationException {
        this.tlsEnabled = properties.getBoolean(TLS_ENABLED);
        this.controllerRestURI = (this.isTlsEnabled() ? "https://" : "http://") + cleanUri(properties.get(CONTROLLER_REST_URI));
        this.controllerGrpcURI = (this.isTlsEnabled() ? "tls://" : "tcp://") + cleanUri(properties.get(CONTROLLER_GRPC_URI));
        this.authEnabled = properties.getBoolean(AUTH_ENABLED);
        this.userName = properties.get(USER_NAME);
        this.password = properties.get(PASSWORD);
        this.truststore = properties.get(TRUSTSTORE_JKS);
        this.accessTokenTtl = properties.getDuration(TRUSTSTORE_ACCESS_TOKEN_TTL_SECONDS, ChronoUnit.SECONDS);
        this.metadataBackend = properties.get(METADATA_BACKEND);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<CLIConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, CLIConfig::new);
    }

    private static String cleanUri(String uri) {
        int pattern = uri.indexOf(PROTOCOL_PATTERN);
        if (pattern != -1) {
            return uri.substring(pattern + PROTOCOL_PATTERN.length());
        }
        return uri;
    }
}
