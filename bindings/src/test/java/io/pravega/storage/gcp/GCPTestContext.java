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
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;

import java.util.UUID;

/**
 * Test context GCP tests.
 */
public class GCPTestContext {
    public static final String BUCKET_NAME_PREFIX = "pravega-pre/";
    public final GCPStorageConfig adapterConfig;

    public final Storage storage;
    public final ChunkedSegmentStorageConfig defaultConfig = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;

    public GCPTestContext() {
        String bucketName = "pravega-unit-test";
        String prefix = BUCKET_NAME_PREFIX + UUID.randomUUID();
        this.adapterConfig = GCPStorageConfig.builder()
                .with(GCPStorageConfig.BUCKET, bucketName)
                .with(GCPStorageConfig.PREFIX, prefix)
                .build();

        storage = LocalStorageHelper.getOptions().getService();
    }

}
