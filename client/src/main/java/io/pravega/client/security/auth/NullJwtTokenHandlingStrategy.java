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
 * Strategy for handling null delegation tokens. This strategy is similar as {@link JwtTokenHandlingStrategy}, except
 * that it does not assume the presence of an initial delegation token.
 */
public class NullJwtTokenHandlingStrategy extends JwtTokenHandlingStrategy {

    public NullJwtTokenHandlingStrategy(@NonNull Controller controllerClient,
                                        @NonNull String scopeName, @NonNull String streamName) {
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