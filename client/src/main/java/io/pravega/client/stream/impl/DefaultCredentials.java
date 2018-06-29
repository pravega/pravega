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

import com.google.common.collect.ImmutableMap;
import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode
public class DefaultCredentials implements Credentials {
    
    private static final long serialVersionUID = 1L;

    private final ImmutableMap<String, String> credsMap;

    public DefaultCredentials(String password, String userName) {
        credsMap = ImmutableMap.of("userName", userName,
                "password", password);
    }

    @Override
    public String getAuthenticationType() {
        return "Pravega-Default";
    }

    @Override
    public Map<String, String> getAuthParameters() {
        return credsMap;
    }
}
