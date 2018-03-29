/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.stream.impl.Credentials;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;

/**
 * This class contains configuration that is passed on to Pravega client.
 * Please note that this is an experimental object and the contents and their interpretation may change
 * in future.
 */
@Data
@Beta
@Builder(toBuilder = true)
public class ClientConfig {
    /** controllerURI The controller rpc URI. This can be of 2 types
     1. tcp://ip1:port1,ip2:port2,...
        This is used if the controller endpoints are static and can be directly accessed.
     2. pravega://ip1:port1,ip2:port2,...
        This is used to autodiscovery the controller endpoints from an initial controller list.
    */
    private final URI controllerURI;

    /**
     * Credentials to be passed on to the Pravega controller for authentication and authorization.
     */
    private final Credentials credentials;

    /**
     * Path to an optional truststore. If this is null or empty, the default JVM trust store is used.
     * This is currently expected to be a signing certificate for the certification authority.
     */
    private final String trustStore;

    /**
     * If the flag {@link #isEnableTls is set, this flag decides whether to enable host name validation or not.
     */
    private boolean validateHostName;

    public boolean isEnableTls() {
        return this.controllerURI.getScheme().equals("tls") || this.controllerURI.getScheme().equals("ssl")
                || this.controllerURI.getScheme().equals("pravegas");
    }

    public static final class ClientConfigBuilder {
        private static final String AUTH_PROPS_START = "pravega.client.auth.";
        private static final String AUTH_METHOD = AUTH_PROPS_START + "method";
        private static final String AUTH_METHOD_LOAD_DYNAMIC = AUTH_PROPS_START + "loadDynamic";

        private static final String AUTH_PROPS_START_ENV = "pravega_client_auth_";

        private boolean validateHostName = true;

        public ClientConfig build() {
            if (controllerURI == null) {
                controllerURI = URI.create("tcp://localhost");
            }
            extractCredentials();
            return new ClientConfig(controllerURI, credentials, trustStore, validateHostName);
        }

        /**
         * Function to extract the credentials object in the given client config.
         * Here is the order of preference in descending order:
         * 1. User provides a credential object. This overrides any other settings.
         * 2. System properties: System properties are defined in the format: "pravega.client.auth.*"
         * 3. Environment variables. Environment variables are defined under the format "PRAVEGA_CLIENT_AUTH_*"
         * 4. In case of option 2 and 3, the caller can decide whether the class needs to be loaded dynamically by
         *     setting property `pravega.client.auth.loadDynamic` to true.
         */
        private void extractCredentials() {
            extractCredentials(System.getProperties(), System.getenv());
        }

        @VisibleForTesting
        ClientConfigBuilder extractCredentials(Properties properties, Map<String, String> env) {
            if (credentials != null) {
                return this;
            }

            credentials = extractCredentialsFromProperties(properties);
            if (credentials == null) {
                credentials = extractCredentialsFromEnv(env);
            }
            return this;
        }

        private Credentials extractCredentialsFromProperties(Properties properties) {
            Map<String, String> retVal = properties.entrySet()
                                                   .stream()
                                                   .filter(entry -> entry.getKey().toString().startsWith(AUTH_PROPS_START))
                                                   .collect(Collectors.toMap(entry ->
                                                                   entry.getKey().toString().replace("_", "."),
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
                                            .filter(entry -> entry.getKey().toString().startsWith(AUTH_PROPS_START_ENV))
                                            .collect(Collectors.toMap(entry -> (String) entry.getKey(), value -> (String) value.getValue()));
            if (retVal.containsKey(AUTH_METHOD)) {
                return credentialFromMap(retVal);
            } else {
                return null;
            }
        }

        private Credentials credentialFromMap(Map<String, String> credsMap) {

            String expectedMethod = credsMap.get(AUTH_METHOD);

            // Load the class dynamically if the user wants it to.
            if (credsMap.containsKey(AUTH_METHOD_LOAD_DYNAMIC) && Boolean.parseBoolean(credsMap.get(AUTH_METHOD_LOAD_DYNAMIC))) {
                ServiceLoader<Credentials> loader = ServiceLoader.load(Credentials.class);
                for (Credentials creds : loader) {
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
                public Map<String, String> getAuthParameters() {
                    return Collections.unmodifiableMap(credsMap);
                }
            };
        }
    }
}
