/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.auth;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.Credentials;
import net.jcip.annotations.GuardedBy;

import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Helper class to extract credentials.
 */
public class CredentialsExtractor {
    private static final String AUTH_PROPS_START = "pravega.client.auth.";
    private static final String AUTH_METHOD = AUTH_PROPS_START + "method";
    private static final String AUTH_METHOD_LOAD_DYNAMIC = AUTH_PROPS_START + "loadDynamic";

    private static final String AUTH_PROPS_START_ENV = "pravega_client_auth_";

    @GuardedBy("$this")
    private final Properties properties;
    @GuardedBy("$this")
    private final Map<String, String> env;

    public CredentialsExtractor() {
        this(System.getProperties(), System.getenv());
    }

    @VisibleForTesting
    CredentialsExtractor(Properties properties, Map<String, String> env) {
        this.properties = properties;
        this.env = env;
    }

    /**
     * Function to extract the credentials object in the given client config.
     * Here is the order of preference in descending order:
     * 1. User provides a credential object. This overrides any other settings.
     * 2. System properties: System properties are defined in the format: "pravega.client.auth.*"
     * 3. Environment variables. Environment variables are defined under the format "PRAVEGA_CLIENT_AUTH_*"
     * 4. In case of option 2 and 3, the caller can decide whether the class needs to be loaded dynamically by
     *     setting property `pravega.client.auth.loadDynamic` to true.
     *
     * @param config the config containing the credentials object.
     * @return Returns a new copy of credentials object. This object is created as per the steps above.
     */
    public ClientConfig extract(ClientConfig config) {
        if (config.getCredentials() != null) {
            return config;
        }

        Credentials credentials = extractCredentialsFromProperties();
        if (credentials == null) {
            credentials = extractCredentialsFromEnv();
        }

        return ClientConfig.builder()
                           .credentials(credentials)
                           .trustStore(config.getTrustStore())
                           .validateHostName(config.isValidateHostName())
                           .controllerURI(config.getControllerURI())
                           .build();

    }

    private Credentials extractCredentialsFromProperties() {
        synchronized (this) {
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
    }

    private Credentials extractCredentialsFromEnv() {
        synchronized (this) {
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
                return credsMap;
            }
        };
    }
}
