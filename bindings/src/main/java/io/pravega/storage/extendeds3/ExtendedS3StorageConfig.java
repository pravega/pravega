/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Config;
import com.emc.object.util.ConfigUri;
import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the ExtendedS3 Storage component.
 */
@Slf4j
public class ExtendedS3StorageConfig {
    //region Config Names

    public static final Property<String> CONFIGURI = Property.named("configUri", "");
    public static final Property<String> BUCKET = Property.named("bucket", "");
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    public static final Property<Boolean> USENONEMATCH = Property.named("useNoneMatch", false);

    private static final String COMPONENT_CODE = "extendeds3";
    private static final String PATH_SEPARATOR = "/";

    //endregion

    //region Members

    /**
     *  The S3 complete client config of the EXTENDEDS3 REST interface
     */
    @Getter
    private final S3Config s3Config;

    /**
     *  The EXTENDEDS3 access key id - this is equivalent to the user
     */
    @Getter
    private final String accessKey;

    /**
     *  The EXTENDEDS3 secret key associated with the accessKey
     */
    @Getter
    private final String secretKey;

    /**
     *  A unique bucket name to store objects
     */
    @Getter
    private final String bucket;

    /**
     * Prefix of the Pravega owned EXTENDEDS3 path under the assigned buckets. All the objects under this path will be
     * exclusively owned by Pravega.
     */
    @Getter
    private final String prefix;

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
        ConfigUri<S3Config> s3ConfigUri = new ConfigUri<S3Config>(S3Config.class);
        this.s3Config = Preconditions.checkNotNull(s3ConfigUri.parseUri(properties.get(CONFIGURI)), "configUri");
        this.accessKey = Preconditions.checkNotNull(s3Config.getIdentity(), "identity");
        this.secretKey = Preconditions.checkNotNull(s3Config.getSecretKey(), "secretKey");
        this.bucket = Preconditions.checkNotNull(properties.get(BUCKET), "bucket");
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
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
