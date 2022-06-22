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

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.test.common.TestUtils;
import net.minidev.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.util.UUID;

/**
 * Test context GCP tests.
 */
public class GCPTestContext {
    public static final String BUCKET_NAME_PREFIX = "pravega-pre/";
    public final GCPStorageConfig adapterConfig;

    public final int port;
    public final String configUri;
    public final Storage storage;
    public final ChunkedSegmentStorageConfig defaultConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;

    public GCPTestContext() {
        this.port = TestUtils.getAvailableListenPort();
        this.configUri = "https://localhost";
        String bucketName = "pravega-unit-test";
        String prefix = BUCKET_NAME_PREFIX + UUID.randomUUID();
        this.adapterConfig = GCPStorageConfig.builder()
                .with(GCPStorageConfig.CONFIGURI, configUri)
                .with(GCPStorageConfig.BUCKET, bucketName)
                .with(GCPStorageConfig.PREFIX, prefix)
                .with(GCPStorageConfig.ACCESS_KEY, "access")
                .with(GCPStorageConfig.SECRET_KEY, "secret")
                .build();
        /*JSONObject jsonObject = getServiceAcountJSON();
        RemoteStorageHelper helper = RemoteStorageHelper.create("pravega-amit", new ByteArrayInputStream(jsonObject.toJSONString().getBytes()));
        storage = helper.getOptions().getService();*/
        storage = LocalStorageHelper.getOptions().getService();
    }

    private JSONObject getServiceAcountJSON() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "service_account");
        jsonObject.put("project_id", "pravega-amit");
        jsonObject.put("private_key_id", "79c7633cdab4e612fd924cb4b1c77d86df907fbd");
        jsonObject.put("private_key", "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDS+QCb5vrCQoei\n6vMhhLDkCjbcFeIwsHIpYPgeh1ZgNc6V99C82pKUFps9PQLLcM4vdZTqKVvKp+wJ\nFn+TeOoCTpIYEfPtzBbqEoU+TzQrvFMMiBPNz4r+40/KvPDn5TfIAaiSIKx+X2wL\ngWKX8/0Fm45bPjbfOIFGdvWBrwebz5bFKTlXFvQemfh62+HTgHSRTyJGSkEled+f\n4Odqji+tUEACOkdli1X51PHm7sjir76sKF2WNG8ksqoiNfO9u3C9K4kqHyh6vJAZ\nrbr0MkVLitq4NiZWe5GoKxX79cXp+ctE2NWyzhb8BjPvDe4knymirwstYLnzuTJ5\noFlhjqXPAgMBAAECggEAQXEd4D5Y4HNUsZOh0W7glAwbEk/zdtj0wKMktAuVHojy\nSRCy/jHqr+cHRoqrWEHoo04c4DnuEEHgdL02257xL8ABj1faS5Q4M2mFTVuyOjLT\nrBp10iyj2AbY1HGhZL10fSUOji12dEjTMgpzc+EqRlgHY4Q77ygO6bWy2ARcHtdI\nn3ImjvLkful2qAfi4KT02VCZ348lCXkoGgsoCARhrM2qV7CmvlJF5wDf/Uoo44kH\n02sU+pRPh70PcCFg3NDcZb2UM3soZznba7DjVgiQSExMgflnDhxPkWcONx9gNDHq\nhVDnwZpNkJRgfwc01pfJI4JywveHx+icwV0iBhFVGQKBgQDtFuTyknQCi4/WaWPH\njdCKndEQHISHojdvSNWBi6p0ItpcOARr3brAyLRQc0cv5miZzHzNVLxCIl/rbWnZ\n0GJO9votrR8WQ5aLPi4w1s1w8JGMnUYROCw8x5dM4BzPJX3i5tDoRFoq8MNAlpKi\nwr5KktnPKQI2bI60SQtkLhvsXQKBgQDjzNUYGQcwkPZXsehk6nqwtpZ5OJTRsiCu\nyRHLTcoinGnjEU9G7DRqguIOkRUQokpZxUbXRWUO+XnBX58epaKorH24nJQgcMUl\niqK5PP2W4PrH6ZMygO617xbH/FuixBrOzQgT4jSdngG3+lQh5DTed10rurKGGp+P\nbf+juQQYGwKBgQC8IcKiyZvMuTn2BcLrgpjMpdZTVo3DovEiGUVyeoVTiqSDMOAx\nR8z9VUXf4NnIJKk0AZO2y1pnkCdVBYlNEZIw3sI+pHVakV9QNpMopgp3aC3WyqXi\n3BQeVrK0idHSfgmal1WGOVbjZBFLmy/Yf3fIbSbwv7XFwfarEJs9b2kw8QKBgGxk\nesEMp68kSxNPRBVAvUB4oQDtO2LML2D7q8vhJ91wL7Ir+lz057wGqynjPvK7RkWQ\n6TRlgMCvVI/+v+gFSHCaIvhFCPamsig631LlAoVYZ/vX2IKfdvZ63YwrOC8qwNbG\nGKHdcMvO82JnasD1pXJ1uY+lNm05HdNRs+Jjlt8hAoGBALPoOQhkl3A51PDU5TGp\npELBoTAZTLPq7qcmH2gnm8BXeIwmDwHbS55TeMs5Qj3u6qJHx0Sa1/eCAaz50UoG\nyHoyr1UjW/BTWN3xXOIO9AOJpSXab/5uTrncmuTLEMLmtarTKH4pFTqQsI6tqSzu\na8cKSUs/ImzmpUXKDrtVUYBC\n-----END PRIVATE KEY-----\n");
        jsonObject.put("client_email", "amitsa@pravega-amit.iam.gserviceaccount.com");
        jsonObject.put("client_id", "113833936441999013298");
        jsonObject.put("auth_uri", "https://accounts.google.com/o/oauth2/auth");
        jsonObject.put("token_uri", "https://oauth2.googleapis.com/token");
        jsonObject.put("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs");
        jsonObject.put("client_x509_cert_url", "https://www.googleapis.com/robot/v1/metadata/x509/amitsa%40pravega-amit.iam.gserviceaccount.com");
        return jsonObject;
    }

}
