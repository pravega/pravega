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

import io.pravega.auth.AuthHandler;
import io.pravega.auth.ServerConfig;

public class TestAuthHandler implements AuthHandler {

    public static final String DUMMY_USER = "dummy";
    public static final String ADMIN_USER = "admin";

    @Override
    public String getHandlerName() {
        return "testHandler";
    }

    @Override
    public boolean authenticate(String token) {
        return true;
    }

    @Override
    public Permissions authorize(String resource, String token) {
        if (token.contains(DUMMY_USER)) {
            return Permissions.NONE;
        } else {
            return Permissions.READ_UPDATE;
        }
    }

    @Override
    public void initialize(ServerConfig serverConfig) {

    }

    public static String testAuthToken(String userName) {
        return String.format("testHandler %s", userName);
    }
}
