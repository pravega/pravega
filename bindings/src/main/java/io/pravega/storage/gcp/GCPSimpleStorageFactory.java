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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import com.google.gson.JsonObject;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for GCP {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link GCPChunkStorage}.
 */
@RequiredArgsConstructor
public class GCPSimpleStorageFactory implements SimpleStorageFactory {

    /**
     * ChunkedSegmentStorageConfig contains configuration for {@link ChunkedSegmentStorage}.
     */
    @NonNull
    @Getter
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    /**
     * GCPStorageConfig contains configuration for GCP.
     */
    @NonNull
    private final GCPStorageConfig config;

    /**
     * ScheduledExecutorService is an {@link java.util.concurrent.ExecutorService} that can schedule commands to run after a given delay.
     */
    @NonNull
    @Getter
    private final ScheduledExecutorService executor;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        return new ChunkedSegmentStorage(containerId,
                createChunkStorage(),
                metadataStore,
                this.executor,
                this.chunkedSegmentStorageConfig);
    }

    /**
     * Creates a new instance of a Storage adapter.
     */
    @Override
    public Storage createStorageAdapter() {
        throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
    }

    @Override
    public ChunkStorage createChunkStorage() {
        com.google.cloud.storage.Storage storage = createStorageOptions(this.config).getService();
        return new GCPChunkStorage(storage, this.config, this.executor, false);
    }

    /**
     * Creates instance of {@link StorageOptions} based on given {@link GCPStorageConfig}.
     *
     * @param config Config to use.
     * @return StorageOptions instance.
     */
    static StorageOptions createStorageOptions(GCPStorageConfig config) {
        GoogleCredentials credentials;
        if (config.isUseMock()) {
            return LocalStorageHelper.getOptions();
        }
        try {
            credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(getServiceAcountJSON().toString().getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return StorageOptions.newBuilder().setCredentials(credentials).build();
    }

    private static JsonObject getServiceAcountJSON() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("type", GCPStorageConfig.ACCOUNT_TYPE.getDefaultValue());
        jsonObject.addProperty("project_id", GCPStorageConfig.PROJECT_ID.getDefaultValue());
        jsonObject.addProperty("private_key_id", GCPStorageConfig.PRIVATE_KEY_ID.getDefaultValue());
        jsonObject.addProperty("private_key", GCPStorageConfig.PRIVATE_KEY.getDefaultValue());
        jsonObject.addProperty("client_email", GCPStorageConfig.CLIENT_EMAIL.getDefaultValue());
        jsonObject.addProperty("client_id", GCPStorageConfig.CLIENT_ID.getDefaultValue());

        return jsonObject;
    }
}
