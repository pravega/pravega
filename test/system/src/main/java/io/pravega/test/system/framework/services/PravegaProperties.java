/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

/**
 * This class encapsulates all system properties/environment to be passed to the operator for a Pravega deployment.
 * */
@Slf4j
public class PravegaProperties {
    private final ImmutableMap<String, String> defaultProperties = ImmutableMap.<String, String>builder()
            // Segment store properties.getExecutionContext
            .put("autoScale.muteInSeconds", "120")
            .put("autoScale.cooldownInSeconds", "120")
            .put("autoScale.cacheExpiryInSeconds", "120")
            .put("autoScale.cacheCleanUpInSeconds", "120")
            .put("curator-default-session-timeout", "10000")
            .put("bookkeeper.bkAckQuorumSize", "3")
            // Controller properties.
            .put("controller.transaction.maxLeaseValue", "60000")
            .put("controller.retention.frequencyMinutes", "2")
            .put("log.level", "DEBUG")
            .build();
    private Map<String, String> properties = new HashMap<String, String>(defaultProperties);

    /**
     * This method adds a key only if it is absent, assuming the default values never need to change
     *
     * */
    private void addProperty(String key, String value) {
        properties.putIfAbsent(key, value);
    }

    public ImmutableMap<String, String> getProperties() {
        return ImmutableMap.copyOf(properties);
    }

    private static void addAuthProperties(PravegaProperties ctx) {
        // controller
        ctx.addProperty("controller.auth.enabled", "true");
        ctx.addProperty("controller.auth.userPasswordFile", "/opt/pravega/conf/passwd");
        ctx.addProperty("controller.auth.tokenSigningKey", "secret");

        //segment store
        ctx.addProperty("autoScale.authEnabled", "true");
        ctx.addProperty("autoScale.tokenSigningKey", "secret");
    }


    @Builder(builderMethodName = "buildWithAuth")
    public static PravegaProperties buildWithAuth(URI uri) {

        log.debug("Auth Builder invoked.");
        PravegaProperties ctx = new PravegaProperties();
        addAuthProperties(ctx);
        return ctx;
    }

    @Builder(builderMethodName = "builder")
    public static PravegaProperties build(URI uri) {
        log.debug("Default Builder invoked.");
        return new PravegaProperties();
    }

}
