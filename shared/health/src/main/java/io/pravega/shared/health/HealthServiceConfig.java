/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import lombok.Getter;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;

/**
 * Configuration for the Health component.
 */
public class HealthServiceConfig {

    public static final Property<String> ADDRESS = Property.named("connect.host.ip", "localhost");
    public static final Property<Integer> PORT = Property.named("connect.host.port", 10090);
    public static final Property<Integer> INTERVAL = Property.named("health.interval.seconds", 10);
    private static final String COMPONENT_CODE = "health";

    @Getter
    private final String address;

    /**
     * The port at which to host the {@link HealthService}.
     */
    @Getter
    private final int port;

    /**
     * The interval at which to perform recurring health checks on the service.
     */
    @Getter
    private final int interval;

    private HealthServiceConfig(TypedProperties properties) throws ConfigurationException {
        this.address = properties.get(ADDRESS);
        this.port = properties.getInt(PORT);
        this.interval = properties.getInt(INTERVAL);
    }

    /**
     * Creates a new {@link ConfigBuilder} used to create instances of {@link HealthServiceConfig}.
     *
     * @return
     */
    public static ConfigBuilder<HealthServiceConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, HealthServiceConfig::new);
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append(String.format("\t%s: %s\n", ADDRESS.getName(), this.address))
                .append(String.format("\t%s: %s\n", PORT.getName(), this.port))
                .append(String.format("\t%s: %s", INTERVAL.getName(), this.interval))
                .toString();
    }
}
