/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import java.util.HashMap;
import java.util.Map;

public class PravegaDefaultCredentials implements PravegaCredentials {
    private final HashMap<String, String> credsMap;

    public PravegaDefaultCredentials(String password, String userName) {
        credsMap = new HashMap<>();
        credsMap.put("userName", userName);
        credsMap.put("password", password);
    }

    @Override
    public String getAuthenticationType() {
        return "Pravega-Default";
    }

    @Override
    public Map<String, String> getAuthHeaders() {
        return credsMap;
    }
}
