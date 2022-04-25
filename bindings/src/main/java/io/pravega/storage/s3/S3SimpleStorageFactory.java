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

import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
public class S3SimpleStorageFactory implements SimpleStorageFactory {
    private static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";
    private static final String AWS_SECRET_ACCESS_KEY = "aws.secretAccessKey";
    private static final String AWS_REGION = "aws.region";

    @NonNull
    @Getter
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @NonNull
    private final S3StorageConfig config;

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
        S3Client s3Client = createS3Client(this.config);
        return new S3ChunkStorage(s3Client, this.config, this.executor, true);
    }

    /**
     * Creates instance of {@link S3Client} based on given {@link S3StorageConfig}.
     * @param config Config to use.
     * @return S3Client instance.
     */
    static S3Client createS3Client(S3StorageConfig config) {
        S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(getCredentialsProvider(config, config.isAssumeRoleEnabled()))
                .region(Region.of(config.getRegion()));
        if (config.isShouldOverrideUri()) {
            builder = builder.endpointOverride(URI.create(config.getS3Config()));
        }
        return builder.build();
    }

    private static AwsCredentialsProvider getCredentialsProvider(S3StorageConfig config, boolean useSession) {
        setSystemProperties(config);
        if (useSession) {
            return getRoleCredentialsProvider(config.getUserRole(), UUID.randomUUID().toString());
        } else {
            return getStaticCredentialsProvider(config);
        }
    }

    private static StaticCredentialsProvider getStaticCredentialsProvider(S3StorageConfig config) {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey());
        return StaticCredentialsProvider.create(credentials);
    }

    private static AwsCredentialsProvider getRoleCredentialsProvider(String roleArn, String roleSessionName) {
        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest.builder()
                .roleArn(roleArn)
                .roleSessionName(roleSessionName)
                .build();

        StsClient stsClient = StsClient.builder().build();

        return StsAssumeRoleCredentialsProvider
                .builder()
                .stsClient(stsClient).refreshRequest(assumeRoleRequest)
                .asyncCredentialUpdateEnabled(true)
                .build();

    }

    private static void setSystemProperties(S3StorageConfig config) {
        System.setProperty(AWS_ACCESS_KEY_ID, config.getAccessKey());
        System.setProperty(AWS_SECRET_ACCESS_KEY, config.getSecretKey());
        System.setProperty(AWS_REGION, config.getRegion());
    }
}
