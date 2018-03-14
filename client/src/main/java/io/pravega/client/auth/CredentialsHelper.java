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

/**
 * Helper class to extract credentials.
 */
public class CredentialsHelper {
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
        return config;

    }
}
