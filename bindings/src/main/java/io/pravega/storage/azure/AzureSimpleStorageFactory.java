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
package io.pravega.storage.azure;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.storage.s3.S3ChunkStorage;
import io.pravega.storage.s3.S3StorageConfig;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for S3 {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link S3ChunkStorage}.
 */
@RequiredArgsConstructor
public class AzureSimpleStorageFactory implements SimpleStorageFactory {
//    private static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
//    private static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";
//    private static final String AWS_REGION = "aws.region";

    @NonNull
    @Getter
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @NonNull
    private final AzureStorageConfig config;

    @NonNull
    @Getter
    private final ScheduledExecutorService executor;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                createChunkStorage(),
                metadataStore,
                this.executor,
                this.chunkedSegmentStorageConfig);
        return chunkedSegmentStorage;
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
        AzureClient azureClient = createAzureClient(this.config);
        return new AzureChunkStorage(azureClient, this.config, this.executor, true, true);
    }

    private AzureClient createAzureClient(AzureStorageConfig config) {
        return null;
    }



//    private static void setSystemProperties(S3StorageConfig config) {
//        System.setProperty(AWS_ACCESS_KEY_ID, config.getAccessKey());
//        System.setProperty(AWS_SECRET_ACCESS_KEY, config.getSecretKey());
//        System.setProperty(AWS_REGION, config.getRegion());
//    }
}
