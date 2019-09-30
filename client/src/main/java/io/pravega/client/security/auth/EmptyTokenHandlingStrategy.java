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

/**
 * Strategy for handling empty delegation tokens.
 */
public class EmptyTokenHandlingStrategy implements DelegationTokenHandlingStrategy {

    @Override
    public String retrieveToken() {
        return "";
    }

    @Override
    public String refreshToken() {
        return "";
    }
}
