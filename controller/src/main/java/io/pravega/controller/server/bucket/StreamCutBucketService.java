package io.pravega.controller.server.bucket;

import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import io.pravega.common.tracing.TagLogger;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.slf4j.LoggerFactory;

public class StreamCutBucketService extends AbstractBucketService {
    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(StreamCutBucketService.class));

    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamMetadataStore streamMetadataStore;
    private final RequestTracker requestTracker;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;

    StreamCutBucketService(int bucketId, StreamMetadataStore streamMetadataStore, BucketStore bucketStore,
                                  StreamMetadataTasks streamMetadataTasks, ScheduledExecutorService executor,
                                  RequestTracker requestTracker) {
        super(bucketId, bucketStore, executor);
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamMetadataStore = streamMetadataStore;
        this.requestTracker = requestTracker;
    }

    @Override
    void registerBucketChangeListener(BucketChangeListener bucketService) {
        bucketStore.registerBucketChangeListenerForRetention(bucketId, this);
    }

    @Override
    public CompletableFuture<List<String>> getStreamsForBucket() {
        return bucketStore.getStreamsForRetention(bucketId, executor);
    }

    CompletableFuture<Void> startWork(StreamImpl stream) {
        // Randomly distribute retention work across RETENTION_FREQUENCY_IN_MINUTES spectrum by introducing a random initial
        // delay. This will ensure that not all streams become eligible for processing of retention at around similar times.
        long delay = Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis();
        long randomInitialDelay = ThreadLocalRandom.current().nextLong(delay);
        return Futures.delayedFuture(() -> performRetention(stream), randomInitialDelay, executor)
                      .thenCompose(x -> RetryHelper.loopWithDelay(this::isRunning, () -> performRetention(stream),
                              delay, executor));
    }

    private CompletableFuture<Void> performRetention(StreamImpl stream) {
        OperationContext context = streamMetadataStore.createContext(stream.getScope(), stream.getStreamName());

        // Track the new request for this automatic truncation.
        long requestId = requestIdGenerator.get();
        String requestDescriptor = RequestTracker.buildRequestDescriptor("truncateStream", stream.getScope(),
                stream.getStreamName());
        requestTracker.trackRequest(requestDescriptor, requestId);
        log.debug(requestId, "Periodic background processing for retention called for stream {}/{}",
                stream.getScope(), stream.getStreamName());

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getConfiguration(stream.getScope(), stream.getStreamName(), context, executor)
                                                                     .thenCompose(config -> streamMetadataTasks.retention(stream.getScope(), stream.getStreamName(),
                                                                             config.getRetentionPolicy(), System.currentTimeMillis(), context,
                                                                             this.streamMetadataTasks.retrieveDelegationToken()))
                                                                     .exceptionally(e -> {
                                                                         log.warn(requestId, "Exception thrown while performing auto retention for stream {} ", stream, e);
                                                                         throw new CompletionException(e);
                                                                     }), RetryHelper.UNCONDITIONAL_PREDICATE, 5, executor)
                          .exceptionally(e -> {
                              log.warn(requestId, "Unable to perform retention for stream {}. " +
                                      "Ignoring, retention will be attempted in next cycle.", stream, e);
                              return null;
                          }).thenRun(() -> requestTracker.untrackRequest(requestDescriptor));
    }
}
