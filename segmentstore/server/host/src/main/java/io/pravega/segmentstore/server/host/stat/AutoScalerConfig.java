/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host.stat;

import com.google.common.base.Strings;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.net.URI;
import java.time.Duration;
import lombok.Data;
import lombok.Getter;

@Data
public class AutoScalerConfig {
    public static final Property<String> REQUEST_STREAM = Property.named("requestStream.name", "_requeststream", "requestStream");
    public static final Property<Integer> COOLDOWN_IN_SECONDS = Property.named("cooldown.time.seconds", 10 * 60, "cooldownInSeconds");
    public static final Property<Integer> MUTE_IN_SECONDS = Property.named("mute.time.seconds", 10 * 60, "muteInSeconds");
    public static final Property<Integer> CACHE_CLEANUP_IN_SECONDS = Property.named("cache.cleanUp.interval.seconds", 5 * 60, "cacheCleanUpInSeconds");
    public static final Property<Integer> CACHE_EXPIRY_IN_SECONDS = Property.named("cache.expiry.seconds", 20 * 60, "cacheExpiryInSeconds");
    public static final Property<String> CONTROLLER_URI = Property.named("controller.connect.uri", "tcp://localhost:9090", "controllerUri");
    public static final Property<Boolean> TLS_ENABLED = Property.named("controller.connect.security.tls.enable", false, "tlsEnabled");
    public static final Property<String> TLS_CERT_FILE = Property.named("controller.connect.security.tls.truststore.location", "", "tlsCertFile");
    public static final Property<Boolean> AUTH_ENABLED = Property.named("controller.connect.security.auth.enable", false, "authEnabled");
    public static final Property<String> TOKEN_SIGNING_KEY = Property.named("security.auth.token.signingKey.basis", "secret", "tokenSigningKey");
    public static final Property<Boolean> VALIDATE_HOSTNAME = Property.named("controller.connect.security.tls.validateHostName.enable", true, "validateHostName");
    public static final Property<Integer> THREAD_POOL_SIZE = Property.named("threadPool.size", 10, "threadPoolSize");

    public static final String COMPONENT_CODE = "autoScale";

    /**
     * Uri for controller.
     */
    @Getter
    private final URI controllerUri;
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

    /**
     * Flag to represent the case where interactions with controller are encrypted with TLS.
     */
    @Getter
    private final boolean tlsEnabled;

    /**
     * The X.509 certificate file used for TLS connection to controller.
     */
    @Getter
    private final String tlsCertFile;

    /**
     * Flag to represent the case where controller expects authorization details.
     */
    @Getter
    private final boolean authEnabled;

    /**
     * Signing key for the auth token.
     */
    @Getter
    private final String tokenSigningKey;

    /**
     * Flag indicating whether to validate the hostname when TLS is enabled.
     */
    @Getter
    private final boolean validateHostName;

    /**
     * The number of threads for the {@link AutoScaleMonitor}.
     */
    @Getter
    private final int threadPoolSize;

    private AutoScalerConfig(TypedProperties properties) throws ConfigurationException {
        this.internalRequestStream = properties.get(REQUEST_STREAM);
        this.cooldownDuration = Duration.ofSeconds(properties.getInt(COOLDOWN_IN_SECONDS));
        this.muteDuration = Duration.ofSeconds(properties.getInt(MUTE_IN_SECONDS));
        this.cacheCleanup = Duration.ofSeconds(properties.getInt(CACHE_CLEANUP_IN_SECONDS));
        this.cacheExpiry = Duration.ofSeconds(properties.getInt(CACHE_EXPIRY_IN_SECONDS));
        this.controllerUri = URI.create(properties.get(CONTROLLER_URI));
        this.tlsEnabled = properties.getBoolean(TLS_ENABLED);
        this.authEnabled = properties.getBoolean(AUTH_ENABLED);
        this.tlsCertFile = properties.get(TLS_CERT_FILE);
        this.tokenSigningKey = properties.get(TOKEN_SIGNING_KEY);
        this.validateHostName = properties.getBoolean(VALIDATE_HOSTNAME);
        this.threadPoolSize = properties.getInt(THREAD_POOL_SIZE);
        if (this.threadPoolSize <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", THREAD_POOL_SIZE));
        }
    }

    public static ConfigBuilder<AutoScalerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, AutoScalerConfig::new);
    }

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return new StringBuilder(String.format("%s(", getClass().getSimpleName()))
                .append(String.format("controllerUri: %s, ", (controllerUri != null) ? controllerUri.toString() : "null"))
                .append(String.format("internalRequestStream: %s, ", internalRequestStream))
                .append(String.format("cooldownDuration: %s, ", (cooldownDuration != null) ? cooldownDuration.toString() : "null"))
                .append(String.format("muteDuration: %s, ", (muteDuration != null) ? muteDuration.toString() : "null"))
                .append(String.format("cacheExpiry: %s, ", (cacheExpiry != null) ? cacheExpiry.toString() : "null"))
                .append(String.format("cacheCleanup: %s, ", (cacheCleanup != null) ? cacheCleanup.toString() : "null"))
                .append(String.format("tlsEnabled: %b, ", tlsEnabled))
                .append(String.format("tlsCertFile is %s, ",
                        Strings.isNullOrEmpty(tlsCertFile) ? "unspecified" : "specified"))
                .append(String.format("authEnabled: %b, ", authEnabled))
                .append(String.format("tokenSigningKey is %s, ",
                        Strings.isNullOrEmpty(tokenSigningKey) ? "unspecified" : "specified"))
                .append(String.format("validateHostName: %b, ", validateHostName))
                .append(String.format("threadPoolSize: %d", threadPoolSize))
                .append(")")
                .toString();
    }
}
