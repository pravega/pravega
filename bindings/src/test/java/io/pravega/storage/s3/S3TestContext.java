/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.storage.s3;

import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.test.common.TestUtils;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.UUID;

/**
 * Test context S3 tests.
 */
public class S3TestContext {
    public static final String BUCKET_NAME_PREFIX = "pravega-unit-test/";
    public final S3StorageConfig adapterConfig;

    public final int port;
    public final String configUri;
    public final S3Client s3Client;
    public final S3Mock s3Mock;
    public final ChunkedSegmentStorageConfig defaultConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;

    public S3TestContext() {
        try {
            this.port = TestUtils.getAvailableListenPort();
            this.configUri = "https://localhost";
            String bucketName = "test-bucket";
            String prefix = BUCKET_NAME_PREFIX + UUID.randomUUID();
            this.adapterConfig = S3StorageConfig.builder()
                    .with(S3StorageConfig.CONFIGURI, configUri)
                    .with(S3StorageConfig.BUCKET, bucketName)
                    .with(S3StorageConfig.PREFIX, prefix)
                    .with(S3StorageConfig.ACCESS_KEY, "access")
                    .with(S3StorageConfig.SECRET_KEY, "secret")
                    .build();
            s3Mock = new S3Mock();
            s3Client = new S3ClientMock(this.s3Mock);
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    public void close() {
        if (null != s3Client) {
            s3Client.close();
        }
    }
}
