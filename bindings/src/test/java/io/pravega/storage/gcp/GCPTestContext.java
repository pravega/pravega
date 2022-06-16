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
package io.pravega.storage.gcp;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.cloud.storage.testing.RemoteStorageHelper;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.test.common.TestUtils;

import java.util.UUID;

/**
 * Test context GCP tests.
 */
public class GCPTestContext {
    public static final String BUCKET_NAME_PREFIX = "pravega-unit-test/";
    public final GCPStorageConfig adapterConfig;

    public final int port;
    public final String configUri;
    public final Storage storage;
    public final ChunkedSegmentStorageConfig defaultConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;

    public GCPTestContext() {
            this.port = TestUtils.getAvailableListenPort();
            this.configUri = "https://localhost";
            String bucketName = "";
            String prefix = BUCKET_NAME_PREFIX + UUID.randomUUID();
            this.adapterConfig = GCPStorageConfig.builder()
                    .with(GCPStorageConfig.CONFIGURI, configUri)
                    .with(GCPStorageConfig.BUCKET, bucketName)
                    .with(GCPStorageConfig.PREFIX, prefix)
                    .with(GCPStorageConfig.ACCESS_KEY, "access")
                    .with(GCPStorageConfig.SECRET_KEY, "secret")
                    .build();
            //storage = RemoteStorageHelper.create().getOptions().getService();
            storage = LocalStorageHelper.getOptions().getService();
    }

}
