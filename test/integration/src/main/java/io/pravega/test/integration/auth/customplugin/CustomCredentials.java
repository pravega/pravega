/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.auth.customplugin;

import io.pravega.client.stream.impl.Credentials;

public class CustomCredentials implements Credentials {

    private static final String TOKEN = "static-token";

    @Override
    public String getAuthenticationType() {
        return CustomAuthHandler.METHOD;
    }

    @Override
    public String getAuthenticationToken() {
        return TOKEN;
    }
}
