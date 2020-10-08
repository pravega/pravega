/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.utils;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Configuration for CLI client, specially related to the Controller service in Pravega.
 */
public final class CLIControllerConfig {

    public enum MetadataBackends {
        SEGMENTSTORE, ZOOKEEPER
    }

    private static final Property<String> CONTROLLER_REST_URI = Property.named("controller.rest.uri", "http://localhost:9091");
    private static final Property<String> CONTROLLER_GRPC_URI = Property.named("controller.grpc.uri", "tcp://localhost:9090");
    private static final Property<Boolean> AUTH_ENABLED = Property.named("security.auth.enable", false);
    private static final Property<String> CONTROLLER_USER_NAME = Property.named("security.auth.credentials.username", "");
    private static final Property<String> CONTROLLER_PASSWORD = Property.named("security.auth.credentials.password", "");
    private static final Property<String> METADATA_BACKEND = Property.named("store.metadata.backend", MetadataBackends.SEGMENTSTORE.name());

    private static final String COMPONENT_CODE = "cli";

    /**
     * The Controller REST URI. Recall to set "http" or "https" depending on the TLS configuration of the Controller.
     */
    @Getter
    private final String controllerRestURI;

    /**
     * The Controller GRPC URI. Recall to set "tcp" or "tls" depending on the TLS configuration of the Controller.
     */
    @Getter
    private final String controllerGrpcURI;

    /**
     * Defines whether or not to use authentication in Controller requests.
     */
    @Getter
    private final boolean authEnabled;

    /**
     * User name if authentication is configured in the Controller.
     */
    @Getter
    private final String userName;

    /**
     * Password if authentication is configured in the Controller.
     */
    @Getter
    private final String password;

    /**
     * Controller metadata backend. At the moment, its values can only be "segmentstore" or "zookeeper".
     */
    @Getter
    private final String metadataBackend;

    private CLIControllerConfig(TypedProperties properties) throws ConfigurationException {
        this.controllerRestURI = properties.get(CONTROLLER_REST_URI);
        this.controllerGrpcURI = properties.get(CONTROLLER_GRPC_URI);
        this.authEnabled = properties.getBoolean(AUTH_ENABLED);
        this.userName = properties.get(CONTROLLER_USER_NAME);
        this.password = properties.get(CONTROLLER_PASSWORD);
        this.metadataBackend = properties.get(METADATA_BACKEND);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<CLIControllerConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, CLIControllerConfig::new);
    }
}
