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

/**
 * Configuration for {@link InteractiveDemo}.
 */
@Data
@Builder
class InteractiveConfig {
    static final String CONTROLLER_URI = "controller-uri";
    static final String DEFAULT_SEGMENT_COUNT = "default-segment-count";
    static final String TIMEOUT_MILLIS = "timeout-millis";
    static final String MAX_LIST_ITEMS = "max-list-items";
    static final String PRETTY_PRINT = "pretty-print";

    private String controllerUri;
    private int defaultSegmentCount;
    private int timeoutMillis;
    private int maxListItems;
    private boolean prettyPrint;

    static InteractiveConfig getDefault() {
        return InteractiveConfig.builder()
                .controllerUri("tcp://localhost:9090")
                .defaultSegmentCount(4)
                .timeoutMillis(60000)
                .maxListItems(1000)
                .prettyPrint(true)
                .build();
    }

    InteractiveConfig set(String propertyName, String value) {
        switch (propertyName) {
            case CONTROLLER_URI:
                setControllerUri(value);
                break;
            case DEFAULT_SEGMENT_COUNT:
                setDefaultSegmentCount(Integer.parseInt(value));
                break;
            case TIMEOUT_MILLIS:
                setTimeoutMillis(Integer.parseInt(value));
                break;
            case MAX_LIST_ITEMS:
                setMaxListItems(Integer.parseInt(value));
                break;
            case PRETTY_PRINT:
                setPrettyPrint(Boolean.parseBoolean(value));
                break;
            default:
                throw new IllegalArgumentException(String.format("Unrecognized property name '%s'.", propertyName));
        }
        return this;
    }

    Map<String, Object> getAll() {
        return ImmutableMap.<String, Object>builder()
                .put(CONTROLLER_URI, getControllerUri())
                .put(DEFAULT_SEGMENT_COUNT, getDefaultSegmentCount())
                .put(TIMEOUT_MILLIS, getTimeoutMillis())
                .put(MAX_LIST_ITEMS, getMaxListItems())
                .put(PRETTY_PRINT, isPrettyPrint())
                .build();
    }
}
