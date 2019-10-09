/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.security.auth;

import io.pravega.client.stream.impl.Controller;
import lombok.NonNull;

/**
 * Provider for handling null initialized tokens. This provider is similar as {@link JwtTokenProviderImpl}, except
 * that it does not assume the presence of an initial delegation token.
 */
public class NullInitializedTokenProviderImpl extends JwtTokenProviderImpl {

    public NullInitializedTokenProviderImpl(@NonNull Controller controllerClient, @NonNull String scopeName, @NonNull String streamName) {
            super(controllerClient, scopeName, streamName);
    }

    @Override
    public String retrieveToken() {
        if (this.getCurrentToken() == null) {
            return this.refreshToken();
        } else {
            return super.retrieveToken();
        }
    }
}