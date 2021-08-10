package io.pravega.storage.chunk;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

@RequiredArgsConstructor
public class ECSChunkSimpleStorageFactory implements SimpleStorageFactory {

    @NonNull
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @NonNull
    private final ECSChunkStorageConfig config;

    @NonNull
    private final ScheduledExecutorService executor;
    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                new ECSChunkStorage(createS3Client(), this.config, this.executor),
                metadataStore,
                this.executor,
                this.chunkedSegmentStorageConfig);
        return chunkedSegmentStorage;
    }

    @Override
    public Executor getExecutor() {
        return executor;
    }

    @Override
    public ChunkedSegmentStorageConfig getChunkedSegmentStorageConfig() {
        return chunkedSegmentStorageConfig;
    }

    @Override
    public Storage createStorageAdapter() {
        throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
    }

    private S3Client createS3Client() {
       return S3Client.builder()
               .region(Region.EU_WEST_2)
               .endpointOverride(config.getEndpoint())
               .credentialsProvider(AnonymousCredentialsProvider.create())
               .overrideConfiguration(ClientOverrideConfiguration.builder()
                       .retryPolicy(RetryPolicy.builder()
                               .numRetries(0)
                               .build())
                       .build()).build();
    }
}
