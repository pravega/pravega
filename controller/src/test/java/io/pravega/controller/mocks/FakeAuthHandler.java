/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.mocks;

import io.pravega.auth.AuthHandler;
import io.pravega.auth.ServerConfig;
import io.pravega.controller.server.rpc.auth.UserPrincipal;

import java.security.Principal;

public class FakeAuthHandler implements AuthHandler {

    public static final String UNPRIVILEGED_USER = "unPrivilegedUser";
    public static final String PRIVILEGED_USER = "privilegedUser";

    @Override
    public String getHandlerName() {
        return "Basic";
    }

    @Override
    public Principal authenticate(String token) {
        return new UserPrincipal(token);
    }

    @Override
    public Permissions authorize(String resource, Principal principal) {
        if (principal.getName().equals(PRIVILEGED_USER)) {
            return Permissions.READ_UPDATE;
        } else {
            return Permissions.NONE;
        }
    }

    @Override
    public void initialize(ServerConfig serverConfig) {

    }

    public static String testAuthToken(String userName) {
        return String.format("testHandler %s", userName);
    }
}