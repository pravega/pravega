/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Data;
import lombok.Getter;

import java.net.URI;
import java.time.Duration;

@Data
public class AutoScalerConfig {
    public static final Property<String> REQUEST_STREAM = Property.named("requestStream", "requeststream");
    public static final Property<String> INTERNAL_SCOPE = Property.named("scope", "pravega");
    public static final Property<Integer> SCALE_COOLDOWN_IN_SECONDS = Property.named("scaleCooldownInSeconds", 10 * 60);
    public static final Property<Integer> SCALE_MUTE_IN_SECONDS = Property.named("scaleMuteInSeconds", 10 * 60);
    public static final Property<Integer> SCALE_CACHE_CLEANUP_IN_SECONDS = Property.named("scaleCacheCleanUpInSeconds", 5 * 60);
    public static final Property<Integer> SCALE_CACHE_EXPIRY_IN_SECONDS = Property.named("scaleCacheExpiryInSeconds", 20 * 60);
    public static final Property<String> CONTROLLER_URI = Property.named("controllerUri", "tcp://localhost:9090");

    private static final String COMPONENT_CODE = "autoScale";

    @Getter
    private final URI controllerUri;
    @Getter
    private final String internalScope;
    @Getter
    private final String internalRequestStream;
    @Getter
    private final Duration cooldownDuration;
    @Getter
    private final Duration muteDuration;
    @Getter
    private final Duration cacheExpiry;
    @Getter
    private final Duration cacheCleanup;

    private AutoScalerConfig(TypedProperties properties) throws ConfigurationException {
        this.internalScope = properties.get(INTERNAL_SCOPE);
        this.internalRequestStream = properties.get(REQUEST_STREAM);
        this.cooldownDuration = Duration.ofSeconds(properties.getInt(SCALE_COOLDOWN_IN_SECONDS));
        this.muteDuration = Duration.ofSeconds(properties.getInt(SCALE_MUTE_IN_SECONDS));
        this.cacheCleanup = Duration.ofSeconds(properties.getInt(SCALE_CACHE_CLEANUP_IN_SECONDS));
        this.cacheExpiry = Duration.ofSeconds(properties.getInt(SCALE_CACHE_EXPIRY_IN_SECONDS));
        this.controllerUri = URI.create(properties.get(CONTROLLER_URI));
    }

    public static ConfigBuilder<AutoScalerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, AutoScalerConfig::new);
    }
}
