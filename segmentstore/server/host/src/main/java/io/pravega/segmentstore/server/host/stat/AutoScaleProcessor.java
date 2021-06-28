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
package io.pravega.segmentstore.server.host.stat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.Retry;
import io.pravega.common.util.SimpleCache;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.controller.event.ControllerEventSerializer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

/**
 * This looks at segment aggregates and determines if a scale operation has to be triggered.
 * If a scale has to be triggered, then it puts a new scale request into the request stream.
 */
public class AutoScaleProcessor implements AutoCloseable {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(AutoScaleProcessor.class));
    private static final EventSerializer SERIALIZER = new EventSerializer();

    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
    private static final long FIVE_MINUTES = Duration.ofMinutes(5).toMillis();
    private static final long TEN_MINUTES = Duration.ofMinutes(10).toMillis();
    private static final long TWENTY_MINUTES = Duration.ofMinutes(20).toMillis();
    private static final int MAX_CACHE_SIZE = 1000000;

    private final EventStreamClientFactory clientFactory;
    private final SimpleCache<String, Pair<Long, Long>> cache;
    private final CompletableFuture<EventStreamWriter<AutoScaleEvent>> writer;
    private final AtomicBoolean startInitWriter;
    private final AutoScalerConfig configuration;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;
    private final ScheduledFuture<?> cacheCleanup;

    /**
     * Creates a new instance of the {@link AutoScaleProcessor} class. This sets up its own {@link EventStreamClientFactory}
     * and {@link EventStreamWriter} instances.
     *
     * @param configuration The {@link AutoScalerConfig} to use as configuration.
     * @param executor      The Executor to use for async operations.
     */
    AutoScaleProcessor(@NonNull AutoScalerConfig configuration, @NonNull ScheduledExecutorService executor) {
        this(configuration, createFactory(configuration), executor);
    }

    /**
     * Creates a new instance of the {@link AutoScaleProcessor} class.
     *
     * @param writer        The {@link EventStreamWriter} instance to use.
     * @param configuration The {@link AutoScalerConfig} to use as configuration.
     * @param executor      The Executor to use for async operations.
     */
    @VisibleForTesting
    AutoScaleProcessor(@NonNull EventStreamWriter<AutoScaleEvent> writer, @NonNull AutoScalerConfig configuration,
                       @NonNull ScheduledExecutorService executor) {
        this(configuration, null, executor);
        this.writer.complete(writer);
    }

    @VisibleForTesting
    AutoScaleProcessor(@NonNull AutoScalerConfig configuration, EventStreamClientFactory clientFactory,
                       @NonNull ScheduledExecutorService executor) {
        this(configuration, clientFactory, executor, null);
    }

    /**
     * Creates a new instance of the {@link AutoScaleProcessor} class.
     *
     * @param configuration The {@link AutoScalerConfig} to use as configuration.
     * @param clientFactory The {@link EventStreamClientFactory} to use to bootstrap {@link EventStreamWriter} instances.
     * @param executor      The Executor to use for async operations.
     */
    @VisibleForTesting
    AutoScaleProcessor(@NonNull AutoScalerConfig configuration, EventStreamClientFactory clientFactory,
                       @NonNull ScheduledExecutorService executor, SimpleCache<String, Pair<Long, Long>> simpleCache) {
        this.configuration = configuration;
        this.writer = new CompletableFuture<>();
        this.clientFactory = clientFactory;
        this.startInitWriter = new AtomicBoolean(false);
        
        if (simpleCache == null) {
            this.cache = new SimpleCache<>(MAX_CACHE_SIZE, configuration.getCacheExpiry(), (k, v) -> triggerScaleDown(k, true));
        } else {
            this.cache = simpleCache;
        }
        // Even if there is no activity, keep cleaning up the cache so that scale down can be triggered.
        // caches do not perform clean up if there is no activity. This is because they do not maintain their
        // own background thread.
        this.cacheCleanup = executor.scheduleAtFixedRate(cache::cleanUp, 0, configuration.getCacheCleanup().getSeconds(), TimeUnit.SECONDS);
        if (clientFactory != null) {
            bootstrapRequestWriters(clientFactory, executor);
        }
    }

    @Override
    public void close() {
        writer.cancel(true);

        if (Futures.isSuccessful(writer)) {
            val w = this.writer.join();
            if (w != null) {
                w.close();
            }
        }

        if (clientFactory != null) {
            this.clientFactory.close();
        }
        this.cacheCleanup.cancel(true);
    }
    
    private void bootstrapRequestWriters(EventStreamClientFactory clientFactory, ScheduledExecutorService executor) {
        AtomicReference<EventStreamWriter<AutoScaleEvent>> w = new AtomicReference<>();

        Futures.completeAfter(() -> Retry.indefinitelyWithExpBackoff(100, 10, 10000, this::handleBootstrapException)
                                         .runInExecutor(() -> bootstrapOnce(clientFactory, w),
                                                 executor).thenApply(v -> w.get()), writer);
    }

    private void handleBootstrapException(Throwable e) {
        if (startInitWriter.get()) {
            log.warn("Unable to create writer for requeststream: {}.", LoggerHelpers.exceptionSummary(log, e));
        }
    }

    @VisibleForTesting
    void bootstrapOnce(EventStreamClientFactory clientFactory, AtomicReference<EventStreamWriter<AutoScaleEvent>> writerRef) {
        if (!writer.isDone()) {
            if (!startInitWriter.get()) {
                throw new RuntimeException("Init not requested");
            }
            // Ensure the writer tries indefinitely to establish connection. This retry will continue in the background
            // until the AutoScaleProcessor is closed.
            EventWriterConfig writerConfig = EventWriterConfig.builder().retryAttempts(Integer.MAX_VALUE).build();
            writerRef.set(clientFactory.createEventWriter(configuration.getInternalRequestStream(),
                    SERIALIZER, writerConfig));
            log.info("AutoScale Processor Initialized. RequestStream={}",
                    configuration.getInternalRequestStream());
        }
    }

    private static EventStreamClientFactory createFactory(AutoScalerConfig configuration) {
        return EventStreamClientFactory.withScope(NameUtils.INTERNAL_SCOPE_NAME, prepareClientConfig(configuration));
    }

    @VisibleForTesting
    static ClientConfig prepareClientConfig(AutoScalerConfig configuration) {
        // Auth credentials are passed via system properties or environment variables at deployment time, when needed.
        ClientConfig.ClientConfigBuilder clientConfigBuilder =
                ClientConfig.builder().controllerURI(configuration.getControllerUri());

        if (configuration.isTlsEnabled()) {
            log.debug("Auto scale TLS is enabled");
            clientConfigBuilder.trustStore(configuration.getTlsCertFile())
                    .validateHostName(configuration.isValidateHostName());

            if (!hasTlsEnabled(configuration.getControllerUri())) {
                log.debug("Auto scale Controller URI [{}] has a non-TLS scheme, although auto scale TLS is enabled",
                        configuration.getControllerUri());
                clientConfigBuilder.enableTlsToSegmentStore(true);
            }
        }
        return clientConfigBuilder.build();
    }

    @VisibleForTesting
    static boolean hasTlsEnabled(@NonNull final URI controllerURI) {
        Preconditions.checkNotNull(controllerURI);
        String uriScheme = controllerURI.getScheme();
        if (uriScheme == null) {
            return false;
        }
        return uriScheme.equals("tls") || uriScheme.equals("pravegas");
    }

    private void triggerScaleUp(String streamSegmentName, int numOfSplits) {
        Pair<Long, Long> pair = cache.get(streamSegmentName);
        long lastRequestTs = 0;

        if (pair != null && pair.getKey() != null) {
            lastRequestTs = pair.getKey();
        }

        long timestamp = getTimeMillis();
        long requestId = requestIdGenerator.get();
        if (timestamp - lastRequestTs > configuration.getMuteDuration().toMillis()) {
            log.info(requestId, "sending request for scale up for {}", streamSegmentName);

            Segment segment = Segment.fromScopedName(streamSegmentName);
            AutoScaleEvent event = new AutoScaleEvent(segment.getScope(), segment.getStreamName(), segment.getSegmentId(),
                    AutoScaleEvent.UP, timestamp, numOfSplits, false, requestId);
            // Mute scale for timestamp for both scale up and down
            writeRequest(event, () -> cache.put(streamSegmentName, new ImmutablePair<>(timestamp, timestamp)));
        }
    }

    private void triggerScaleDown(String streamSegmentName, boolean silent) {
        Pair<Long, Long> pair = cache.get(streamSegmentName);
        long lastRequestTs = 0;

        if (pair != null && pair.getValue() != null) {
            lastRequestTs = pair.getValue();
        }

        long timestamp = getTimeMillis();
        long requestId = requestIdGenerator.get();
        if (timestamp - lastRequestTs > configuration.getMuteDuration().toMillis()) {
            log.info(requestId, "sending request for scale down for {}", streamSegmentName);

            Segment segment = Segment.fromScopedName(streamSegmentName);
            AutoScaleEvent event = new AutoScaleEvent(segment.getScope(), segment.getStreamName(), segment.getSegmentId(),
                    AutoScaleEvent.DOWN, timestamp, 0, silent, requestId);
            writeRequest(event, () -> {
                if (!silent) {
                    // mute only scale downs
                    cache.put(streamSegmentName, new ImmutablePair<>(0L, timestamp));
                }
            });
        }
    }

    private void writeRequest(AutoScaleEvent event, Runnable successCallback) {
        startInitWriter.set(true);
        writer.thenCompose(w -> w.writeEvent(event.getKey(), event)
                    .whenComplete((r, e) -> {
                        if (e != null) {
                            log.error(event.getRequestId(), "Unable to post Scale Event to RequestStream '{}'.",
                                    this.configuration.getInternalRequestStream(), e);
                        } else {
                            log.debug(event.getRequestId(), "Scale Event posted successfully: {}.", event);
                            successCallback.run();
                        }
                    }));
    }

    void report(String streamSegmentName, long targetRate, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
        log.info("received traffic for {} with twoMinute rate = {} and targetRate = {}", streamSegmentName, twoMinuteRate, targetRate);
        cache.get(streamSegmentName);
        // note: we are working on caller's thread. We should not do any blocking computation here and return as quickly as
        // possible.
        // So we will decide whether to scale or not and then unblock by asynchronously calling 'writeEvent'
        long currentTime = getTimeMillis();
        if (currentTime - startTime > configuration.getCooldownDuration().toMillis()) {
            log.debug("cool down period elapsed for {}", streamSegmentName);

            // report to see if a scale operation needs to be performed.
            if ((twoMinuteRate > 5.0 * targetRate && currentTime - startTime > TWO_MINUTES) ||
                    (fiveMinuteRate > 2.0 * targetRate && currentTime - startTime > FIVE_MINUTES) ||
                    (tenMinuteRate > targetRate && currentTime - startTime > TEN_MINUTES)) {
                int numOfSplits = Math.max(2, (int) (Double.max(Double.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate));
                log.debug("triggering scale up for {} with number of splits {}", streamSegmentName, numOfSplits);

                triggerScaleUp(streamSegmentName, numOfSplits);
            }

            if (twoMinuteRate < targetRate &&
                    fiveMinuteRate < targetRate &&
                    tenMinuteRate < targetRate &&
                    twentyMinuteRate < targetRate / 2.0 &&
                    currentTime - startTime > TWENTY_MINUTES) {
                log.debug("triggering scale down for {}", streamSegmentName);

                triggerScaleDown(streamSegmentName, false);
            }
        }
    }

    void notifyCreated(String segmentStreamName) {
        long timeMillis = getTimeMillis();
        cache.put(segmentStreamName, new ImmutablePair<>(timeMillis, timeMillis));
    }

    void notifySealed(String segmentStreamName) {
        cache.remove(segmentStreamName);
    }

    @VisibleForTesting
    void put(String streamSegmentName, ImmutablePair<Long, Long> lrImmutablePair) {
        cache.put(streamSegmentName, lrImmutablePair);
    }

    @VisibleForTesting
    Pair<Long, Long> get(String streamSegmentName) {
        return cache.get(streamSegmentName);
    }
    
    @VisibleForTesting
    CompletableFuture<EventStreamWriter<AutoScaleEvent>> getWriterFuture() {
        return writer;
    }

    @VisibleForTesting
    boolean isInitializeStarted() {
        return startInitWriter.get();
    }

    private static class EventSerializer implements Serializer<AutoScaleEvent> {
        private final ControllerEventSerializer baseSerializer = new ControllerEventSerializer();

        @Override
        public ByteBuffer serialize(AutoScaleEvent value) {
            return this.baseSerializer.toByteBuffer(value);
        }

        @Override
        public AutoScaleEvent deserialize(ByteBuffer serializedValue) {
            return (AutoScaleEvent) this.baseSerializer.fromByteBuffer(serializedValue);
        }
    }

    @VisibleForTesting
    protected long getTimeMillis() {
        return System.currentTimeMillis();
    }
}

