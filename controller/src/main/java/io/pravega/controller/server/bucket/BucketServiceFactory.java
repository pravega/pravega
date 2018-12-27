package io.pravega.controller.server.bucket;

import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

@Slf4j
public class BucketServiceFactory {
    private final String hostId;
    private final BucketStore bucketStore;
    private final StreamMetadataStore streamStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executorService;
    private final RequestTracker requestTracker;

    public BucketServiceFactory(String hostId, BucketStore bucketStore, StreamMetadataStore streamStore, 
                                StreamMetadataTasks streamMetadataTasks, ScheduledExecutorService executorService, 
                                RequestTracker requestTracker) {
        this.hostId = hostId;
        this.bucketStore = bucketStore;
        this.streamStore = streamStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executorService = executorService;
        this.requestTracker = requestTracker;
    }

    public BucketManager getBucketManagerService(BucketStore.ServiceType type) {
        BucketManager manager;
        switch (type) {
            case RetentionService:
                Function<Integer, AbstractBucketService> streamCutSupplier = bucket ->
                        new RetentionBucketService(bucket, streamStore, bucketStore, streamMetadataTasks, executorService, requestTracker);

                manager = new BucketManager(hostId, bucketStore, BucketStore.ServiceType.RetentionService,
                        executorService, streamCutSupplier);
                break;
                default:
                    throw new IllegalArgumentException();
        }
        
        return manager;
    }
}
