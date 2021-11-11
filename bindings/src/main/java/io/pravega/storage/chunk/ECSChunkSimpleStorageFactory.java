package io.pravega.storage.chunk;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.storage.chunk.netty.NettyConnection;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
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
        return new ChunkedSegmentStorage(containerId,
                this.config.isNettyClient()? new ECSChunkNettyStorage(createNettyClient(), this.config, this.executor) :new ECSChunkObjectStorage(createS3Clients(), this.config, this.executor),
                metadataStore,
                this.executor,
                this.chunkedSegmentStorageConfig);
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

    private List<NettyConnection> createNettyClient() {
        List<NettyConnection> clients = new ArrayList<>();
        int i = 0;
        for (URI endpoint : config.getEndpoints()) {
            clients.add(new NettyConnection("netty-conn-"+i++, endpoint, config));
        }
        return clients;
    }

    private List<S3Client> createS3Clients() {
        List<S3Client> clients = new ArrayList<>();
        for (URI endpoint : config.getEndpoints()) {
            clients.add(S3Client.builder()
                    .region(Region.EU_WEST_2)
                    .endpointOverride(endpoint)
                    .credentialsProvider(AnonymousCredentialsProvider.create())
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .retryPolicy(RetryPolicy.builder()
                                    .numRetries(0)
                                    .build())
                            .build()).build());
        }
       return clients;
    }
}
