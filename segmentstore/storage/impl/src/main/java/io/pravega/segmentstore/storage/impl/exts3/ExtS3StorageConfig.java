/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.exts3;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.net.URI;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the ExtS3 Storage component.
 */
@Slf4j
public class ExtS3StorageConfig {
    //region Config Names

    public static final Property<String> ROOT = Property.named("Root", "/");

    public static final Property<String> EXTS3_ACCESS_KEY_ID = Property.named( "exts3AccessKey", "");

    public static final Property<String> EXTS3_SECRET_KEY = Property.named( "exts3SecretKey", "");

    public static final Property<String> EXTS3_URI = Property.named( "exts3Url", "");

    public static final Property<String> EXTS3_BUCKET = Property.named("exts3Bucket", "pravega-bucket");

    public static final Property<String> EXTS3_NAMESPACE = Property.named("exts3Namespace", ""); // use default namespace


    private static final String COMPONENT_CODE = "exts3";

    //endregion

    //region Members

    /**
     * Root of the Pravega owned EXTS3 path under the assigned buckets. All the objects under this path will be
     * exclusively owned by Pravega.
     */
    @Getter
    private final String exts3Root;

    /**
     *  The EXTS3 access key id - this is equivalent to the user
     */
    @Getter
    private final String exts3AccessKey;

    /**
     *  The EXTS3 secret key associated with the EXTS3_ACCESS_KEY_ID
     */
    @Getter
    private final String exts3SecretKey;

    /**
     *  The end point of the EXTS3 REST interface
     */
    @Getter
    private final URI exts3Url;

    /**
     *  A unique bucket name to store objects
     */
    @Getter
    private final String exts3Bucket;

    /**
     *  The optional namespace within EXTS3 - leave blank to use the default namespace
     */
    @Getter
    private final String exts3Namespace;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the NFSStorageConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private ExtS3StorageConfig(TypedProperties properties) throws ConfigurationException {
        this.exts3Root = properties.get(ROOT);
        this.exts3AccessKey = properties.get(EXTS3_ACCESS_KEY_ID);
        this.exts3SecretKey = properties.get(EXTS3_SECRET_KEY);
        this.exts3Url = URI.create(properties.get(EXTS3_URI));
        this.exts3Bucket = properties.get(EXTS3_BUCKET);
        this.exts3Namespace = properties.get(EXTS3_NAMESPACE);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<ExtS3StorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, ExtS3StorageConfig::new);
    }

    //endregion
}
