/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.gcp;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Configuration for the GCP Storage component.
 */
@Slf4j
public class GCPStorageConfig {
    //region Config Names
    public static final Property<Boolean> OVERRIDE_CONFIGURI = Property.named("connect.config.uri.override", false);
    public static final Property<String> CONFIGURI = Property.named("connect.config.uri", "", "configUri");
    public static final Property<String> ACCESS_KEY = Property.named("connect.config.access.key", "");
    public static final Property<String> SECRET_KEY = Property.named("connect.config.secret.key", "");
    public static final Property<String> BUCKET = Property.named("bucket", "");
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    public static final Property<Boolean> USENONEMATCH = Property.named("noneMatch.enable", false, "useNoneMatch");
    public static final Property<Boolean> ASSUME_ROLE = Property.named("connect.config.assumeRole.enable", false);
    public static final Property<String> USER_ROLE = Property.named("connect.config.role", "");
    private static final String COMPONENT_CODE = "gcp";
    private static final String PATH_SEPARATOR = "/";

    //endregion

    //region Members

    /**
     *  The GCP complete client config of the GCP REST interface
     */
    @Getter
    private final String gcpConfig;

    /**
     *  The GCP region to use
     */
    @Getter
    private final String region;

    /**
     *  The GCP access key id - this is equivalent to the user
     */
    @Getter
    private final String accessKey;

    /**
     *  The GCP secret key associated with the accessKey
     */
    @Getter
    private final String secretKey;

    /**
     *  A unique bucket name to store objects
     */
    @Getter
    private final String bucket;

    /**
     * Prefix of the Pravega owned GCP path under the assigned buckets. All the objects under this path will be
     * exclusively owned by Pravega.
     */
    @Getter
    private final String prefix;

    /**
     * Whether to use if-none-match header or not.
     */
    @Getter
    private final boolean useNoneMatch;

    /**
     * Whether to use end point other than default.
     */
    @Getter
    private final boolean shouldOverrideUri;

    /**
     * Whether to use STS tokens by using assume role.
     */
    @Getter
    private final boolean assumeRoleEnabled;

    /**
     *  The role to assume.
     */
    @Getter
    private final String userRole;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the GCPStorageConfigConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private GCPStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.shouldOverrideUri = properties.getBoolean(OVERRIDE_CONFIGURI);
        this.gcpConfig = Preconditions.checkNotNull(properties.get(CONFIGURI), "configUri");
        this.region = "";
        this.accessKey = Preconditions.checkNotNull(properties.get(ACCESS_KEY), "accessKey");
        this.secretKey = Preconditions.checkNotNull(properties.get(SECRET_KEY), "secretKey");
        this.bucket = Preconditions.checkNotNull(properties.get(BUCKET), "bucket");
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
        this.useNoneMatch = properties.getBoolean(USENONEMATCH);
        this.assumeRoleEnabled = properties.getBoolean(ASSUME_ROLE);
        this.userRole = Preconditions.checkNotNull(properties.get(USER_ROLE), "userRole");
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<GCPStorageConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, GCPStorageConfig::new);
    }

    //endregion
}
