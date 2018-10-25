/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.net.URI;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the ExtendedS3 Storage component.
 */
@Slf4j
public class ExtendedS3StorageConfig {
    //region Config Names

    public static final Property<String> ROOT = Property.named("root", "/");
    public static final Property<String> ACCESS_KEY_ID = Property.named("accessKey", "");
    public static final Property<String> SECRET_KEY = Property.named("secretKey", "");
    public static final Property<String> URI = Property.named("url", "");
    public static final Property<String> BUCKET = Property.named("bucket", "");
    public static final Property<String> NAMESPACE = Property.named("namespace", ""); // use default namespace
    public static final Property<Boolean> USENONEMATCH = Property.named("useNoneMatch", false);

    private static final String COMPONENT_CODE = "extendeds3";

    //endregion

    //region Members

    /**
     * Root of the Pravega owned EXTENDEDS3 path under the assigned buckets. All the objects under this path will be
     * exclusively owned by Pravega.
     */
    @Getter
    private final String root;

    /**
     *  The EXTENDEDS3 access key id - this is equivalent to the user
     */
    @Getter
    private final String accessKey;

    /**
     *  The EXTENDEDS3 secret key associated with the ACCESS_KEY_ID
     */
    @Getter
    private final String secretKey;

    /**
     *  The end point of the EXTENDEDS3 REST interface
     */
    @Getter
    private final URI url;

    /**
     *  A unique bucket name to store objects
     */
    @Getter
    private final String bucket;

    /**
     *  The optional namespace within EXTENDEDS3 - leave blank to use the default namespace
     */
    @Getter
    private final String namespace;

    /**
     *
     */
    @Getter
    private final boolean useNoneMatch;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ExtendedS3StorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ExtendedS3StorageConfig(TypedProperties properties) throws ConfigurationException {
        this.root = properties.get(ROOT);
        this.accessKey = properties.get(ACCESS_KEY_ID);
        this.secretKey = properties.get(SECRET_KEY);
        this.url = java.net.URI.create(properties.get(URI));
        this.bucket = properties.get(BUCKET);
        this.namespace = properties.get(NAMESPACE);
        this.useNoneMatch = properties.getBoolean(USENONEMATCH);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ExtendedS3StorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ExtendedS3StorageConfig::new);
    }

    //endregion
}
