/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.ext.ContextResolver;

/**
 * Class to define custom json parser for the Jersey web application.
 */
public class CustomObjectMapperProvider implements ContextResolver<ObjectMapper> {

    final ObjectMapper defaultObjectMapper;

    /**
     * Creates an instance of the JSON parser.
     */
    public CustomObjectMapperProvider() {
        defaultObjectMapper = createDefaultMapper();
    }

    @Override
    public ObjectMapper getContext(Class<?> type) {
        return defaultObjectMapper;
    }

    private static ObjectMapper createDefaultMapper() {
        final ObjectMapper result = new ObjectMapper();

        // Allow extra unknown fields in the JSON objects.
        result.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        result.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        return result;
    }
}
