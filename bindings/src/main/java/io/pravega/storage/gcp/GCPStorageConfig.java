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
import org.apache.commons.text.StringEscapeUtils;

/**
 * Configuration for the GCP (Google Cloud Platform) Storage component.
 */
public class GCPStorageConfig {

    //region Config Names
    public static final Property<String> ACCOUNT_TYPE = Property.named("account.type", "service_account");
    public static final Property<String> PROJECT_ID = Property.named("project.id", "");
    public static final Property<String> PRIVATE_KEY_ID = Property.named("private.key.id", "");
    public static final Property<String> PRIVATE_KEY = Property.named("private.key", "");
    public static final Property<String> CLIENT_EMAIL = Property.named("client.email", "");
    public static final Property<String> CLIENT_ID = Property.named("client.id", "");

    public static final Property<String> BUCKET = Property.named("bucket", "");
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    public static final Property<Boolean> USE_MOCK = Property.named("useMock", false);
    private static final String COMPONENT_CODE = "gcp";
    private static final String PATH_SEPARATOR = "/";

    //endregion

    /**
     * The GCP use mock. Keep always false in production.
     */
    @Getter
    boolean useMock;

    /**
     * The access token is a string representation of Google service account json.
     */
    @Getter
    private final String accountType;

    /**
     * The GCP project id.
     */
    @Getter
    private final String projectId;

    /**
     * The GCP private key id.
     */
    @Getter
    private final String privateKeyId;

    /**
     * The GCP private key.
     */
    @Getter
    private final String privateKey;

    /**
     * The GCP client email.
     */
    @Getter
    private final String clientEmail;

    /**
     * The GCP client id.
     */
    @Getter
    private final String clientId;

    /**
     * A unique bucket name to store objects.
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
        this.privateKey = StringEscapeUtils.unescapeJava(Preconditions.checkNotNull(properties.get(PRIVATE_KEY)));
        this.clientEmail = Preconditions.checkNotNull(properties.get(CLIENT_EMAIL));
        this.clientId = Preconditions.checkNotNull(properties.get(CLIENT_ID));
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
