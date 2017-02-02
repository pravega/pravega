/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.service.monitor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
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
        // TODO: read these from configuration.
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
    public void process(String streamSegmentName, boolean autoScale, long targetRate, byte rateType, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {

        // process to see if a scale operation needs to be performed.
        if (twoMinuteRate > 5 * targetRate ||
                fiveMinuteRate > 2 * targetRate ||
                tenMinuteRate > targetRate) {
            int numOfSplits = (int) (Double.max(Double.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate);
            triggerScaleUp(streamSegmentName, numOfSplits);
        }

        if (twoMinuteRate < targetRate &&
                fiveMinuteRate < targetRate &&
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
            Segment segment = Segment.fromScopedName(streamSegmentName);
            ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.UP, timestamp, numOfSplits);
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
            Segment segment = Segment.fromScopedName(streamSegmentName);
            ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.DOWN, timestamp, 0);
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

