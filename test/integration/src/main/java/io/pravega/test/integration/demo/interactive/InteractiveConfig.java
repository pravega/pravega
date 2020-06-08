/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo.interactive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
class InteractiveConfig {
    static final String CONTROLLER_HOST = "controller-host";
    static final String CONTROLLER_PORT = "controller-port";

    static InteractiveConfig getDefault() {
        return InteractiveConfig.builder()
                .controllerHost("localhost")
                .controllerPort(12345)
                .build();
    }

    private String controllerHost;
    private int controllerPort;

    InteractiveConfig set(String propertyName, String value) {
        switch (propertyName) {
            case CONTROLLER_HOST:
                setControllerHost(value);
                break;
            case CONTROLLER_PORT:
                setControllerPort(Integer.parseInt(value));
                break;
            default:
                throw new IllegalArgumentException(String.format("Unrecognized property name '%s'.", propertyName));
        }
        return this;
    }

    Map<String, Object> getAll() {
        return ImmutableMap.<String, Object>builder()
                .put("controller-host", getControllerHost())
                .put("controller-port", getControllerPort())
                .build();
    }
}
