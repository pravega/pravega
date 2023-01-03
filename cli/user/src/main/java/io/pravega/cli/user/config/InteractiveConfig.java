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
package io.pravega.cli.user.config;

import ch.qos.logback.classic.Level;
import com.google.common.collect.ImmutableMap;
import io.pravega.cli.user.UserCLIRunner;
import lombok.Builder;
import lombok.Data;

import java.util.Map;

/**
 * Configuration for {@link UserCLIRunner}.
 */
@Data
@Builder
public class InteractiveConfig {
    public static final String LOG_LEVEL = "log-level";
    public static final String CONTROLLER_URI = "controller-uri";
    public static final String DEFAULT_SEGMENT_COUNT = "default-segment-count";
    public static final String TIMEOUT_MILLIS = "timeout-millis";
    public static final String MAX_LIST_ITEMS = "max-list-items";
    public static final String PRETTY_PRINT = "pretty-print";
    public static final String ROLLOVER_SIZE_BYTES = "rollover-size-bytes";

    public static final String AUTH_ENABLED = "auth-enabled";
    public static final String CONTROLLER_USER_NAME = "auth-username";
    public static final String CONTROLLER_PASSWORD = "auth-password";
    public static final String TLS_ENABLED = "tls-enabled";
    public static final String TRUSTSTORE_JKS = "truststore-location";

    private String controllerUri;
    private int defaultSegmentCount;
    private int timeoutMillis;
    private int maxListItems;
    private boolean prettyPrint;
    private boolean authEnabled;
    private String userName;
    private String password;
    private boolean tlsEnabled;
    private String truststore;
    private long rolloverSizeBytes;
    private Level logLevel;

    public static InteractiveConfig getDefault(Map<String, String> env) {
        //Default tls based configurations for pravega cli
        boolean tlsEnabled = false;
        String systemEnvURI = env.get("PRAVEGA_CONTROLLER_URI");
        String controllerURI = systemEnvURI != null ? systemEnvURI : "localhost:9090";
        if (controllerURI.startsWith("tls://")) {
            tlsEnabled = true;
            controllerURI = controllerURI.replace("tls://", "");
        } else if (controllerURI.startsWith("tcp://")) {
            controllerURI = controllerURI.replace("tcp://", "");
        }
        return InteractiveConfig.builder()
                .controllerUri(controllerURI)
                .defaultSegmentCount(4)
                .timeoutMillis(60000)
                .maxListItems(1000)
                .prettyPrint(true)
                .authEnabled(false)
                .userName("")
                .password("")
                .tlsEnabled(tlsEnabled)
                .truststore("")
                .rolloverSizeBytes(0)
                .logLevel(Level.ERROR)
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
            case AUTH_ENABLED:
                setAuthEnabled(Boolean.parseBoolean(value));
                break;
            case CONTROLLER_USER_NAME:
                setUserName(value);
                break;
            case CONTROLLER_PASSWORD:
                setPassword(value);
                break;
            case TLS_ENABLED:
                setTlsEnabled(Boolean.parseBoolean(value));
                break;
            case TRUSTSTORE_JKS:
                setTruststore(value);
                break;
            case ROLLOVER_SIZE_BYTES:
                setRolloverSizeBytes(Long.parseLong(value));
                break;
            case LOG_LEVEL:
                setLogLevel(Level.toLevel(value));
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
                .put(AUTH_ENABLED, isAuthEnabled())
                .put(CONTROLLER_USER_NAME, getUserName())
                .put(CONTROLLER_PASSWORD, getPassword())
                .put(TLS_ENABLED, isTlsEnabled())
                .put(TRUSTSTORE_JKS, getTruststore())
                .put(ROLLOVER_SIZE_BYTES, getRolloverSizeBytes())
                .put(LOG_LEVEL, getLogLevel())
                .build();
    }
}
