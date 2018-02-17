/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.auth;

import io.pravega.auth.PravegaAuthHandler;
import java.util.Map;

public class TestAuthHandler implements PravegaAuthHandler {

    @Override
    public String getHandlerName() {
        return "testHandler";
    }

    @Override
    public boolean authenticate(Map<String, String> headers) {
        return true;
    }

    @Override
    public PravegaAccessControlEnum authorize(String resource, Map<String, String> headers) {
        return PravegaAccessControlEnum.READ_UPDATE;
    }

    @Override
    public void initialize(Object serverConfig) {

    }
}
