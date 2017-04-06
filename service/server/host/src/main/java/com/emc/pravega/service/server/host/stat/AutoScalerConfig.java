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
    public static final Property<String> INTERNAL_SCOPE = Property.named("scope", "_system");
    public static final Property<String> REQUEST_STREAM = Property.named("requestStream", "_requeststream");
    public static final Property<Integer> COOLDOWN_IN_SECONDS = Property.named("cooldownInSeconds", 10 * 60);
    public static final Property<Integer> MUTE_IN_SECONDS = Property.named("muteInSeconds", 10 * 60);
    public static final Property<Integer> CACHE_CLEANUP_IN_SECONDS = Property.named("cacheCleanUpInSeconds", 5 * 60);
    public static final Property<Integer> CACHE_EXPIRY_IN_SECONDS = Property.named("cacheExpiryInSeconds", 20 * 60);
    public static final Property<String> CONTROLLER_URI = Property.named("controllerUri", "tcp://localhost:9090");

    private static final String COMPONENT_CODE = "autoScale";

    /**
     * Uri for controller.
     */
    @Getter
    private final URI controllerUri;
    /**
     * Scope name for request stream.
     */
    @Getter
    private final String internalScope;
    /**
     * Stream on which scale requests have to be posted.
     */
    @Getter
    private final String internalRequestStream;
    /**
     * Duration for which no scale operation is attempted on a segment after its creation.
     */
    @Getter
    private final Duration cooldownDuration;
    /**
     * Duration for which scale requests for a segment are to be muted.
     * Mute duration is per request type (scale up and scale down). It means if a scale down request was posted
     * for a segment, we will wait until the mute duration before posting the request for the same segment
     * again in the request stream.
     */
    @Getter
    private final Duration muteDuration;
    /**
     * Duration for which a segment lives in auto scaler cache, after which it is expired and a scale down request with
     * silent flag is sent for the segment.
     */
    @Getter
    private final Duration cacheExpiry;
    /**
     * Periodic time period for scheduling auto-scaler cache clean up. Since guava cache does not maintain its own executor,
     * we need to keep performing periodic cache maintenance activities otherwise caller's thread using the cache will be used
     * for cache maintenance.
     * This also ensures that if there is no traffic in the cluster, all segments that have expired are cleaned up from the cache
     * and their respective removal code is invoked.
     */
    @Getter
    private final Duration cacheCleanup;

    private AutoScalerConfig(TypedProperties properties) throws ConfigurationException {
        this.internalScope = properties.get(INTERNAL_SCOPE);
        this.internalRequestStream = properties.get(REQUEST_STREAM);
        this.cooldownDuration = Duration.ofSeconds(properties.getInt(COOLDOWN_IN_SECONDS));
        this.muteDuration = Duration.ofSeconds(properties.getInt(MUTE_IN_SECONDS));
        this.cacheCleanup = Duration.ofSeconds(properties.getInt(CACHE_CLEANUP_IN_SECONDS));
        this.cacheExpiry = Duration.ofSeconds(properties.getInt(CACHE_EXPIRY_IN_SECONDS));
        this.controllerUri = URI.create(properties.get(CONTROLLER_URI));
    }

    public static ConfigBuilder<AutoScalerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, AutoScalerConfig::new);
    }
}
