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
    public static final Property<String> ACCOUNT_TYPE = Property.named("account.type", "service_account");
    public static final Property<String> PROJECT_ID = Property.named("project.id", "");
    public static final Property<String> PRIVATE_KEY_ID = Property.named("private.key.id", "");
    public static final Property<String> PRIVATE_KEY = Property.named("private.key", "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDS+QCb5vrCQoei\n6vMhhLDkCjbcFeIwsHIpYPgeh1ZgNc6V99C82pKUFps9PQLLcM4vdZTqKVvKp+wJ\nFn+TeOoCTpIYEfPtzBbqEoU+TzQrvFMMiBPNz4r+40/KvPDn5TfIAaiSIKx+X2wL\ngWKX8/0Fm45bPjbfOIFGdvWBrwebz5bFKTlXFvQemfh62+HTgHSRTyJGSkEled+f\n4Odqji+tUEACOkdli1X51PHm7sjir76sKF2WNG8ksqoiNfO9u3C9K4kqHyh6vJAZ\nrbr0MkVLitq4NiZWe5GoKxX79cXp+ctE2NWyzhb8BjPvDe4knymirwstYLnzuTJ5\noFlhjqXPAgMBAAECggEAQXEd4D5Y4HNUsZOh0W7glAwbEk/zdtj0wKMktAuVHojy\nSRCy/jHqr+cHRoqrWEHoo04c4DnuEEHgdL02257xL8ABj1faS5Q4M2mFTVuyOjLT\nrBp10iyj2AbY1HGhZL10fSUOji12dEjTMgpzc+EqRlgHY4Q77ygO6bWy2ARcHtdI\nn3ImjvLkful2qAfi4KT02VCZ348lCXkoGgsoCARhrM2qV7CmvlJF5wDf/Uoo44kH\n02sU+pRPh70PcCFg3NDcZb2UM3soZznba7DjVgiQSExMgflnDhxPkWcONx9gNDHq\nhVDnwZpNkJRgfwc01pfJI4JywveHx+icwV0iBhFVGQKBgQDtFuTyknQCi4/WaWPH\njdCKndEQHISHojdvSNWBi6p0ItpcOARr3brAyLRQc0cv5miZzHzNVLxCIl/rbWnZ\n0GJO9votrR8WQ5aLPi4w1s1w8JGMnUYROCw8x5dM4BzPJX3i5tDoRFoq8MNAlpKi\nwr5KktnPKQI2bI60SQtkLhvsXQKBgQDjzNUYGQcwkPZXsehk6nqwtpZ5OJTRsiCu\nyRHLTcoinGnjEU9G7DRqguIOkRUQokpZxUbXRWUO+XnBX58epaKorH24nJQgcMUl\niqK5PP2W4PrH6ZMygO617xbH/FuixBrOzQgT4jSdngG3+lQh5DTed10rurKGGp+P\nbf+juQQYGwKBgQC8IcKiyZvMuTn2BcLrgpjMpdZTVo3DovEiGUVyeoVTiqSDMOAx\nR8z9VUXf4NnIJKk0AZO2y1pnkCdVBYlNEZIw3sI+pHVakV9QNpMopgp3aC3WyqXi\n3BQeVrK0idHSfgmal1WGOVbjZBFLmy/Yf3fIbSbwv7XFwfarEJs9b2kw8QKBgGxk\nesEMp68kSxNPRBVAvUB4oQDtO2LML2D7q8vhJ91wL7Ir+lz057wGqynjPvK7RkWQ\n6TRlgMCvVI/+v+gFSHCaIvhFCPamsig631LlAoVYZ/vX2IKfdvZ63YwrOC8qwNbG\nGKHdcMvO82JnasD1pXJ1uY+lNm05HdNRs+Jjlt8hAoGBALPoOQhkl3A51PDU5TGp\npELBoTAZTLPq7qcmH2gnm8BXeIwmDwHbS55TeMs5Qj3u6qJHx0Sa1/eCAaz50UoG\nyHoyr1UjW/BTWN3xXOIO9AOJpSXab/5uTrncmuTLEMLmtarTKH4pFTqQsI6tqSzu\na8cKSUs/ImzmpUXKDrtVUYBC\n-----END PRIVATE KEY-----\n");
    public static final Property<String> CLIENT_EMAIL = Property.named("client.email", "");
    public static final Property<String> CLIENT_ID = Property.named("client.id", "");

    public static final Property<String> BUCKET = Property.named("bucket", "");
    public static final Property<String> PREFIX = Property.named("prefix", "/");
    public static final Property<Boolean> USE_MOCK = Property.named("useMock", false);
    private static final String COMPONENT_CODE = "gcp";
    private static final String PATH_SEPARATOR = "/";

    //endregion

    /**
     * The GCP use mock. Keep always false in production
     */
    @Getter
    boolean useMock;

    /**
     * The access token is a string representation of google service account json.
     */
    @Getter
    private final String accountType;

    /**
     * The GCP project id
     */
    @Getter
    private final String projectId;

    /**
     * The gcp private key id
     */
    @Getter
    private final String privateKeyId;

    /**
     * The GCP private key
     */
    @Getter
    private final String privateKey;

    /**
     * The GCP client email
     */
    @Getter
    private final String clientEmail;

    /**
     * The GCP client id.
     */
    @Getter
    private final String clientId;

    /**
     * A unique bucket name to store objects
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
        log.info("In GCPStorageConfig constructor");
        this.accountType = Preconditions.checkNotNull(properties.get(ACCOUNT_TYPE));
        this.projectId = Preconditions.checkNotNull(properties.get(PROJECT_ID));
        log.info("projectId=" + projectId);
        this.privateKeyId = Preconditions.checkNotNull(properties.get(PRIVATE_KEY_ID));
        log.info("privateKeyId=" + privateKeyId);
        this.privateKey = Preconditions.checkNotNull(properties.get(PRIVATE_KEY));
        this.clientEmail = Preconditions.checkNotNull(properties.get(CLIENT_EMAIL));
        this.clientId = Preconditions.checkNotNull(properties.get(CLIENT_ID));
        this.bucket = Preconditions.checkNotNull(properties.get(BUCKET), "bucket");
        String givenPrefix = Preconditions.checkNotNull(properties.get(PREFIX), "prefix");
        this.prefix = givenPrefix.endsWith(PATH_SEPARATOR) ? givenPrefix : givenPrefix + PATH_SEPARATOR;
        this.useMock = properties.getBoolean(USE_MOCK);
        log.info("Properties" + properties.toString());
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
