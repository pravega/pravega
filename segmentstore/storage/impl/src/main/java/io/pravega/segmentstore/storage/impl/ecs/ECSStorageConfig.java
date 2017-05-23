/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.ecs;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the NFS Storage component.
 */
@Slf4j
public class ECSStorageConfig {
    //region Config Names

    public static final Property<String> ROOT = Property.named("ecsRoot", "/");

    public static final Property<String> ECS_ACCESS_KEY_ID = Property.named( "ecsAccessKey", "");

    public static final Property<String> ECS_SECRET_KEY = Property.named( "ecsSecretKey", "");

    public static final Property<String> ECS_URI = Property.named( "ecsUrl", "https://object.ecstestdrive.com");

    public static final Property<String> ECS_BUCKET = Property.named("ecsBucket", "pravega-bucket");

    public static final Property<String> ECS_NAMESPACE = Property.named("ecsNamespace", ""); // use default namespace


    private static final String COMPONENT_CODE = "ecs";

    //endregion

    //region Members

    /**
     * Root of the Pravega owned ECS path under the assigned buckets. All the objects under this path will be
     * exclusively owned by Pravega.
     */
    @Getter
    private final String ecsRoot;

    /**
     *  The ECS access key id - this is equivalent to the user
     */
    @Getter
    private final String ecsAccessKey;

    /**
     *  The ECS secret key associated with the ECS_ACCESS_KEY_ID
     */
    @Getter
    private final String ecsSecretKey;

    /**
     *  The end point of the ECS ECS REST interface
     */
    @Getter
    private final String ecsUrl;

    /**
     *  A unique bucket name to store objects
     */
    @Getter
    private final String ecsBucket;

    /**
     *  The optional namespace within ECS - leave blank to use the default namespace
     */
    @Getter
    private final String ecsNamespace;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the NFSStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ECSStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.ecsRoot = properties.get(ROOT);
        this.ecsAccessKey = properties.get(ECS_ACCESS_KEY_ID);
        this.ecsSecretKey = properties.get(ECS_SECRET_KEY);
        this.ecsUrl = properties.get(ECS_URI);
        this.ecsBucket = properties.get(ECS_BUCKET);
        this.ecsNamespace = properties.get(ECS_NAMESPACE);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ECSStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ECSStorageConfig::new);
    }

    //endregion
}
