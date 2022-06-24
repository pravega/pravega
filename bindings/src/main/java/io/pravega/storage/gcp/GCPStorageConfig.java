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
    public static final Property<String> ACCOUNT_TYPE = Property.named("type", "service_account");
    public static final Property<String> PROJECT_ID = Property.named("project_id", "");
    public static final Property<String> PRIVATE_KEY_ID = Property.named("private_key_id", "");
    public static final Property<String> PRIVATE_KEY = Property.named("private_key", "");
    public static final Property<String> CLIENT_EMAIL = Property.named("client_email", "");
    public static final Property<String> CLIENT_ID = Property.named("client_id", "");
    public static final Property<String> AUTH_URI = Property.named("auth_uri", "https://accounts.google.com/o/oauth2/auth");
    public static final Property<String> TOKEN_URI = Property.named("token_uri", "https://oauth2.googleapis.com/token");
    public static final Property<String> AUTH_PROVIDER_CERT_URL = Property.named("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs");
    public static final Property<String> CLIENT_CERT_URL = Property.named("client_x509_cert_url", "");



    public static final Property<String> BUCKET = Property.named("bucket", "");
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    public static final Property<Boolean> USE_MOCK = Property.named("useMock", false);
    private static final String COMPONENT_CODE = "gcp";
    private static final String PATH_SEPARATOR = "/";

    //endregion

    /**
     * The GCP use mock
     */
    @Getter
    boolean useMock;

    /**
     *  The GCP account type
     */
    @Getter
    private final String accountType;

    /**
     *  The GCP projectId
     */
    @Getter
    private final String projectId;

    /**
     *  The GCP
     */
    @Getter
    private final String privateKeyId;

    /**
     *  The GCP
     */
    @Getter
    private final String privateKey;

    /**
     *  The GCP
     */
    @Getter
    private final String clientEmail;

    /**
     *  The GCP
     */
    @Getter
    private final String clientId;

    /**
     *  The GCP
     */
    @Getter
    private final String authUri;

    /**
     *  The GCP
     */
    @Getter
    private final String tokenUri;

    /**
     *  The GCP
     */
    @Getter
    private final String authProviderCertUrl;

    /**
     *  The GCP
     */
    @Getter
    private final String clientCertUrl;

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

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the GCPStorageConfigConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private GCPStorageConfig(TypedProperties properties) throws ConfigurationException {
        this.accountType = Preconditions.checkNotNull(properties.get(ACCOUNT_TYPE));
        this.projectId = Preconditions.checkNotNull(properties.get(PROJECT_ID));
        this.privateKeyId = Preconditions.checkNotNull(properties.get(PRIVATE_KEY_ID));
        this.privateKey = Preconditions.checkNotNull(properties.get(PRIVATE_KEY));
        this.clientEmail = Preconditions.checkNotNull(properties.get(CLIENT_EMAIL));
        this.clientId = Preconditions.checkNotNull(properties.get(CLIENT_ID));
        this.authUri = Preconditions.checkNotNull(properties.get(AUTH_URI));
        this.tokenUri = Preconditions.checkNotNull(properties.get(TOKEN_URI));
        this.authProviderCertUrl = Preconditions.checkNotNull(properties.get(AUTH_PROVIDER_CERT_URL));
        this.clientCertUrl = Preconditions.checkNotNull(properties.get(CLIENT_CERT_URL));
        this.bucket = Preconditions.checkNotNull(properties.get(BUCKET), "bucket");
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
        this.useMock = properties.getBoolean(USE_MOCK);

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
