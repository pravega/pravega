package com.emc.pravega.service.monitor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ThresholdMonitor implements SegmentTrafficMonitor {

    private static final long MUTE_DURATION = Duration.ofMinutes(10).toMillis();
    private static ThresholdMonitor singletonMonitor;

    ClientFactory clientFactory;

    EventStreamWriter<Serializable> writer;

    static SegmentTrafficMonitor getMonitor() {
        if (singletonMonitor == null) {
            singletonMonitor = new ThresholdMonitor();
        }
        return singletonMonitor;
    }

    private ThresholdMonitor() {
        // TODO: read these from configuration
        clientFactory = new ClientFactoryImpl("pravega", URI.create("tcp://controller:9090"));
        writer = clientFactory.createEventWriter("ScaleRequest",
                new JavaSerializer<>(),
                new EventWriterConfig(null));
    }

    // static guava cache <last request ts>
    // forced eviction rule: 20 minutes
    // eviction: if segmentSealed do nothing
    //           else send scale down request

    // LoadingCache
    // event writer
    Cache<String, Pair<Long, Long>> cache = CacheBuilder.newBuilder()
            .initialCapacity(1000)
            .maximumSize(1000000)
            .expireAfterAccess(20, TimeUnit.MINUTES)
            .removalListener((RemovalListener<String, Pair<Long, Long>>) notification -> {
                if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                    triggerScaleDown(notification.getKey());
                }
            })
            .build();

    @Override
    public void process(String streamSegmentName, boolean autoScale, long targetRate, byte rateType, long twoMinuteRate, long fiveMinuteRate, long tenMinuteRate, long twentyMinuteRate) {

        // process to see if a scale operation needs to be performed.
        if (twoMinuteRate > 5 * targetRate ||
                fiveMinuteRate > 2 * targetRate ||
                tenMinuteRate > targetRate) {
            int numOfSplits = (int) (Long.max(Long.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate);
            triggerScaleUp(streamSegmentName, numOfSplits);
        }

        if (twoMinuteRate < 5 * targetRate &&
                fiveMinuteRate < 2 * targetRate &&
                tenMinuteRate < targetRate &&
                twentyMinuteRate < targetRate / 2) {
            triggerScaleDown(streamSegmentName);
        }
    }

    private void triggerScaleUp(String streamSegmentName, int numOfSplits) {
        Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
        long lastRequestTs = 0;

        if (pair != null && pair.getKey() != null) {
            lastRequestTs = pair.getKey();
        }

        long timestamp = System.currentTimeMillis();

        if (timestamp - lastRequestTs > MUTE_DURATION) {
            // TODO: get stream name from streamSegmentName
            String scope;
            String stream;
            int number;
            ScaleRequest event = new ScaleRequest(scope, stream, number, ScaleRequest.UP, timestamp, numOfSplits);
            // Mute scale for timestamp for both scale up and down
            cache.put(streamSegmentName, new ImmutablePair<>(timestamp, timestamp));
            writer.writeEvent(event.getKey(), event);
        }
    }

    private void triggerScaleDown(String streamSegmentName) {
        Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
        long lastRequestTs = 0;

        if (pair != null && pair.getValue() != null) {
            lastRequestTs = pair.getValue();
        }

        long timestamp = System.currentTimeMillis();
        if (timestamp - lastRequestTs > MUTE_DURATION) {
            String scope;
            String stream;
            int number;

            ScaleRequest event = new ScaleRequest(scope, stream, number, ScaleRequest.DOWN, timestamp, 0);
            writer.writeEvent(event.getKey(), event);
        }
    }

    @Override
    public void notify(String segmentStreamName, NotificationType type) {
        // cache.remove
        switch (type) {
            case SegmentCreated: {
                cache.put(segmentStreamName, new ImmutablePair<>(System.currentTimeMillis(), System.currentTimeMillis()));
            }
            break;
            case SegmentSealed: {
                cache.invalidate(segmentStreamName);
            }
            break;
        }
    }
}

