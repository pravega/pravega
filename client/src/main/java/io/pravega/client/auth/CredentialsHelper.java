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

import io.pravega.client.ClientConfig;
import io.pravega.client.stream.impl.Credentials;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class to extract credentials.
 */
public class CredentialsHelper {
    private static final String AUTH_PROPS_START = "pravega.client.auth.";
    private static final String AUTH_METHOD = AUTH_PROPS_START + "method";
    private static final String AUTH_PROPS_START_ENV = "pravega_client_auth_";

    /**
     * Function to extract the credentials object in the given client config.
     * Here is the order of preference in descending order:
     * 1. User provides a credential object. This overrides any other settings.
     * 2. System properties: System properties are defined in the format: "pravega.client.auth.*"
     * 3. Environment variables. Environment variables are defined under the format "PRAVEGA_CLIENT_AUTH_*"
     *
     * @param config the config containing the credentials object.
     */
    public static ClientConfig extractCredentials(ClientConfig config) {
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

    private static Credentials extractCredentialsFromProperties() {
        Map<String, String> retVal = System.getProperties().entrySet()
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

    private static Credentials extractCredentialsFromEnv() {
        Map<String, String> retVal = System.getenv().entrySet()
                                           .stream()
                                           .filter(entry -> entry.getKey().toString().startsWith(AUTH_PROPS_START_ENV))
                                           .collect(Collectors.toMap(entry -> (String) entry.getKey(), value -> (String) value.getValue()));
        if (retVal.containsKey(AUTH_METHOD)) {
            return credentialFromMap(retVal);
        } else {
            return null;
        }
    }

    private static Credentials credentialFromMap(Map<String, String> retVal) {
        return new Credentials() {
            @Override
            public String getAuthenticationType() {
             return retVal.get(AUTH_METHOD);
            }

            @Override
            public Map<String, String> getAuthParameters() {
                return retVal;
            }
        };
    }
}
