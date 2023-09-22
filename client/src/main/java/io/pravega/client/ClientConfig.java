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
package io.pravega.client;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.shared.security.auth.Credentials;
import io.pravega.shared.metrics.MetricListener;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * This class contains configuration that is passed on to Pravega client.
 * Please note that this is an experimental object and the contents and their interpretation may change
 * in future.
 */
@Slf4j
@Data
@Beta
@Builder(toBuilder = true)
public class ClientConfig implements Serializable {

    static final int DEFAULT_MAX_CONNECTIONS_PER_SEGMENT_STORE = 10;
    static final long DEFAULT_CONNECT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    private static final long serialVersionUID = 1L;
    // Use this scheme when client want to connect to a static set of controller servers.
    // Eg: tcp://ip1:port1,ip2:port2
    private final static String SCHEME_DIRECT = "tcp";
    // Secure versions of the direct scheme.
    private final static String SCHEME_DIRECT_TLS = "tls";
    private final static String SCHEME_DIRECT_SSL = "ssl";

    // Use this scheme when client only knows a subset of controllers and wants other controller instances
    // To be auto discovered.
    // Eg: pravega://ip1:port1,ip2:port2
    private final static String SCHEME_DISCOVER = "pravega";
    // Secure version of discover scheme.
    private final static String SCHEME_DISCOVER_TLS = "pravegas";

    /** controllerURI The controller rpc URI. This can be of 2 types
     * 1. tcp://ip1:port1,ip2:port2,...
     *    This is used if the controller endpoints are static and can be directly accessed.
     * 2. pravega://ip1:port1,ip2:port2,...
     *   This is used to autodiscovery the controller endpoints from an initial controller list.
     *
     * @param controllerURI The controller RPC URI.
     * @return The controller RPC URI.
    */
    private final URI controllerURI;

    /**
     * Credentials to be passed on to the Pravega controller for authentication and authorization.
     *
     * @param credentials Pravega controller credentials for authentication and authorization.
     * @return Pravega controller credentials for authentication and authorization.
     */
    private final Credentials credentials;

    /**
     * Path to an optional truststore. If this is null or empty, the default JVM trust store is used.
     * This is currently expected to be a signing certificate for the certification authority.
     *
     * @param trustStore Path to an optional truststore.
     * @return Path to an optional truststore.
     */
    private final String trustStore;

    /**
     * If the flag {@link #isEnableTls()} is set, this flag decides whether to enable host name validation or not.
     *
     * @param validateHostName Flag to decide whether to enable host name validation or not.
     * @return Flag to decide whether to enable host name validation or not.
     */
    private final boolean validateHostName;

    /**
     * Maximum number of connections per Segment store to be used by connection pooling.
     *
     * @return Maximum number of connections per Segment store to be used by connection pooling.
     */
    private final int maxConnectionsPerSegmentStore;

    /**
     * Maximum Connection timeout in milliseconds for establishing connections.
     *
     * @return Connection timeout in milliseconds for establishing connections.
     */
    private final long connectTimeoutMilliSec;


    /**
     * An internal property that determines if the client config.
     *
     * @param isDefaultMaxConnections determines if the client config
     */
    @Getter(AccessLevel.PACKAGE)
    private final boolean isDefaultMaxConnections;

    /**
     * An internal property that determines whether TLS enabled is derived from Controller URI. It cannot be set
     * directly by the caller. It is interpreted as:
     *
     * false - if {@link #enableTlsToController} and/or {@link #enableTlsToSegmentStore} is/are set, and if either
     *         of them is set to false.
     * true - if neither {@link #enableTlsToController} or {@link #enableTlsToSegmentStore} are set, or if both are
     *        set to true.
     */
    @Getter(AccessLevel.NONE) // Omit Lombok accessor
    private final boolean deriveTlsEnabledFromControllerURI;

    /**
     * An optional property representing whether to enable TLS for client's communication with the Controller.
     *
     * If this property and {@link #enableTlsToSegmentStore} are not set, and the scheme used in {@link #controllerURI}
     * is {@code tls} or {@code pravegas}, TLS is automatically enabled for both client-to-Controller and
     * client-to-Segment Store communications.
     */
    @Getter(AccessLevel.NONE) // Omit Lombok accessor as we are creating a custom one
    private final boolean enableTlsToController;

    /**
     * An optional property representing whether to enable TLS for client's communication with the Controller.
     *
     * If this property and {@link #enableTlsToController} are not set, and the scheme used in {@link #controllerURI}
     * is {@code tls} or {@code pravegas}, TLS is automatically enabled for both client-to-Controller and
     * client-to-Segment Store communications.
     */
    @Getter(AccessLevel.NONE) // Omit Lombok accessor as we are creating a custom one
    private final boolean enableTlsToSegmentStore;

    /**
     * An optional listener which can be used to get performance metrics from the client. The user
     * can implement this interface to obtain performance metrics of the client.
     *
     * @param metricListener Listener to collect client performance metrics.
     * @return Listener to collect client performance metrics.
     */
    private final MetricListener metricListener;

    /**
     * Returns whether TLS is enabled for client-to-server (Controller and Segment Store) communications.
     *
     * @return {@code true} if TLS is enabled, otherwise returns {@code false}
     */
    public boolean isEnableTls() {
        if (deriveTlsEnabledFromControllerURI) {
            String scheme = this.controllerURI.getScheme();
            if (scheme == null) { // scheme is null when URL is of the kind //<hostname>:<port>
                return false;
            }
            return scheme.equals("tls") || scheme.equals("ssl") || scheme.equals("pravegas");
        } else {
            return enableTlsToController && enableTlsToSegmentStore;
        }
    }

    public boolean isEnableTlsToController() {
        if (deriveTlsEnabledFromControllerURI) {
            return this.isEnableTls();
        } else {
            return this.enableTlsToController;
        }
    }

    public boolean isEnableTlsToSegmentStore() {
        if (deriveTlsEnabledFromControllerURI) {
            return this.isEnableTls();
        } else {
            return enableTlsToSegmentStore;
        }
    }

    /**
     * This class overrides the lombok builder. It adds some custom functionality on top of the builder.
     * The additional behaviors include:
     * 1. Defining a default controller URI when none is declared/
     * 2. Extracting the credentials object from system properties/environment in following order of descending preference:
     *       a. User provides a credential object. This overrides any other settings.
     *       b. System properties: System properties are defined in the format: "pravega.client.auth.*"
     *       c. Environment variables. Environment variables are defined under the format "pravega_client_auth_*"
     *       d. In case of option 2 and 3, the caller can decide whether the class needs to be loaded dynamically by
     *           setting property `pravega.client.auth.loadDynamic` to true.
     *
     */
    public static final class ClientConfigBuilder {
        private static final String AUTH_PROPS_PREFIX = "pravega.client.auth.";
        private static final String AUTH_METHOD = "method";
        private static final String AUTH_METHOD_LOAD_DYNAMIC = "loadDynamic";
        private static final String AUTH_TOKEN = "token";
        private static final String AUTH_PROPS_PREFIX_ENV = "pravega_client_auth_";

        private boolean validateHostName = true;

        private boolean deriveTlsEnabledFromControllerURI = true;
        private boolean isDefaultMaxConnections = true;

        /**
         * Note: by making this method private, we intend to hide the corresponding property
         * "deriveTlsEnabledFromControllerURI".
         *
         * @param value the value to set
         * @return the builder
         */
        private ClientConfigBuilder deriveTlsEnabledFromControllerURI(boolean value) {
            this.deriveTlsEnabledFromControllerURI = value;
            return this;
        }

        /**
         * Sets the connection timeout in milliseconds for establishing connections.
         *
         * @param connectTimeoutMilliSec The connection timeout in milliseconds for establishing connections.
         * @return the builder.
         */
        public ClientConfigBuilder connectTimeoutMilliSec(long connectTimeoutMilliSec) {
            this.connectTimeoutMilliSec = connectTimeoutMilliSec;
            return this;
        }

        public ClientConfigBuilder maxConnectionsPerSegmentStore(int maxConnectionsPerSegmentStore) {
            if (this.isDefaultMaxConnections) {
                this.isDefaultMaxConnections(false);
                this.maxConnectionsPerSegmentStore = maxConnectionsPerSegmentStore;
            } else {
                log.warn("Update to maxConnectionsPerSegmentStore configuration from {} to {} is ignored,", this.maxConnectionsPerSegmentStore, maxConnectionsPerSegmentStore);
            }
            return this;
        }

        public ClientConfigBuilder enableTlsToController(boolean value) {
            this.deriveTlsEnabledFromControllerURI(false);
            this.enableTlsToController = value;
            return this;
        }

        public ClientConfigBuilder enableTlsToSegmentStore(boolean value) {
            this.deriveTlsEnabledFromControllerURI(false);
            this.enableTlsToSegmentStore = value;
            return this;
        }

        public ClientConfig build() {
            if (controllerURI == null) {
                controllerURI = URI.create("tcp://localhost:9090");
            }
            String scheme = controllerURI.getScheme();
            // If Scheme name is missing in the controllerURI then will add tcp as default scheme.
            // Otherwise if Scheme is not one of the valid scheme then will throw IllegalArgumentException
            if (!isValidScheme(scheme)) {
                if (controllerURI.getScheme() != null && controllerURI.getHost() == null) {
                    controllerURI = URI.create(SCHEME_DIRECT + "://" + controllerURI.toString());
                    log.info("Scheme name is missing in the ControllerURI, therefore adding the default scheme {} to it", SCHEME_DIRECT);
                } else {
                    log.warn("ControllerURI is having unsupported scheme {}.", scheme);
                    throw new IllegalArgumentException("Expected Schemes:  [" + SCHEME_DIRECT + ", " + SCHEME_DIRECT_SSL
                            + ", " + SCHEME_DIRECT_TLS + ", " + SCHEME_DISCOVER + ", " + SCHEME_DISCOVER_TLS
                            + "] but was: " + scheme);
                }
            }
            extractCredentials();
            if (maxConnectionsPerSegmentStore <= 0) {
                maxConnectionsPerSegmentStore = DEFAULT_MAX_CONNECTIONS_PER_SEGMENT_STORE;
            }
            if (connectTimeoutMilliSec <= 0) {
                connectTimeoutMilliSec = DEFAULT_CONNECT_TIMEOUT_MS;
            }
            return new ClientConfig(controllerURI, credentials, trustStore, validateHostName, maxConnectionsPerSegmentStore, connectTimeoutMilliSec,
                    isDefaultMaxConnections, deriveTlsEnabledFromControllerURI, enableTlsToController,
                    enableTlsToSegmentStore, metricListener);
        }

        private boolean isValidScheme(String scheme) {
            return Stream.of(SCHEME_DISCOVER, SCHEME_DISCOVER_TLS, SCHEME_DIRECT, SCHEME_DIRECT_SSL, SCHEME_DIRECT_TLS)
                .anyMatch(x -> x.equals(scheme));
        }

        /**
         * Function to extract the credentials object in the given client config.
         * Here is the order of preference in descending order:
         * 1. User provides a credential object. This overrides any other settings.
         * 2. System properties: System properties are defined in the format: "pravega.client.auth.*"
         * 3. Environment variables. Environment variables are defined under the format "pravega_client_auth_*"
         * 4. In case of option 2 and 3, the caller can decide whether the class needs to be loaded dynamically by
         *     setting property `pravega.client.auth.loadDynamic` to true.
         */
        private void extractCredentials() {
            extractCredentials(System.getProperties(), System.getenv());
        }

        @VisibleForTesting
        ClientConfigBuilder extractCredentials(Properties properties, Map<String, String> env) {
            if (credentials != null) {
                log.info("Client credentials were extracted using the explicitly supplied credentials object.");
                return this;
            }
            if (properties != null) {
                credentials = extractCredentialsFromProperties(properties);
                if (credentials != null) {
                    log.info("Client credentials were extracted from system properties. {}",
                            "They weren't explicitly supplied as a Credentials object.");
                    return this;
                }
            }
            if (env != null) {
                credentials = extractCredentialsFromEnv(env);
                if (credentials != null) {
                    log.info("Client credentials were extracted from environment variables. {}",
                            "They weren't explicitly supplied as a Credentials object or system properties.");
                    return this;
                }
            }
            return this;
        }

        private Credentials extractCredentialsFromProperties(Properties properties) {
            Map<String, String> retVal = properties.entrySet()
                                                   .stream()
                                                   .filter(entry -> entry.getKey().toString().startsWith(AUTH_PROPS_PREFIX))
                                                   .collect(Collectors.toMap(entry -> entry.getKey().toString()
                                                                                           .substring(AUTH_PROPS_PREFIX.length()),
                                                                    value -> (String) value.getValue()));
            if (retVal.containsKey(AUTH_METHOD)) {
                return credentialFromMap(retVal);
            } else {
                return null;
            }
        }

        private Credentials extractCredentialsFromEnv(Map<String, String> env) {
            Map<String, String> retVal = env.entrySet()
                                            .stream()
                                            .filter(entry -> entry.getKey().toString().startsWith(AUTH_PROPS_PREFIX_ENV))
                                            .collect(Collectors.toMap(entry -> entry.getKey().toString()
                                                                                     .replace("_", ".")
                                                                                     .substring(AUTH_PROPS_PREFIX.length()),
                                                    value -> value.getValue()));
            if (retVal.containsKey(AUTH_METHOD)) {
                return credentialFromMap(retVal);
            } else {
                return null;
            }
        }

        // We are using the deprecated legacy interface below: `io.pravega.client.stream.impl.Credentials`.
        @SuppressWarnings("deprecation")
        private Credentials credentialFromMap(Map<String, String> credsMap) {

            String expectedMethod = credsMap.get(AUTH_METHOD);

            // Load the class dynamically if the user wants it to.
            if (credsMap.containsKey(AUTH_METHOD_LOAD_DYNAMIC) && Boolean.parseBoolean(credsMap.get(AUTH_METHOD_LOAD_DYNAMIC))) {
                // Check implementations of the new interface
                ServiceLoader<Credentials> loader = ServiceLoader.load(Credentials.class);
                for (Credentials creds : loader) {
                    if (creds.getAuthenticationType().equals(expectedMethod)) {
                        return creds;
                    }
                }

                // Check implementations of the old interface
                ServiceLoader<io.pravega.client.stream.impl.Credentials> legacyCredentialsLoader =
                        ServiceLoader.load(io.pravega.client.stream.impl.Credentials.class);
                for (Credentials creds : legacyCredentialsLoader) {
                    if (creds.getAuthenticationType().equals(expectedMethod)) {
                        return creds;
                    }
                }
                return null;
            }

            return new Credentials() {
                @Override
                public String getAuthenticationType() {
                    return credsMap.get(AUTH_METHOD);
                }

                @Override
                public String getAuthenticationToken() {
                    return credsMap.get(AUTH_TOKEN);
                }
            };
        }
    }
}
