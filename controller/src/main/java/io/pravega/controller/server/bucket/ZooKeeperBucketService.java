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
package io.pravega.controller.server.bucket;

import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("deprecation")
@Slf4j
public class ZooKeeperBucketService extends BucketService {
    private final ZookeeperBucketStore bucketStore;
    private final AtomicReference<PathChildrenCache> cacheRef;
    
    ZooKeeperBucketService(BucketStore.ServiceType serviceType, int bucketId, ZookeeperBucketStore bucketStore,
                           ScheduledExecutorService executor, int maxConcurrentExecutions, Duration executionPeriod, BucketWork bucketWork) {
        super(serviceType, bucketId, executor, maxConcurrentExecutions, executionPeriod, bucketWork);
        this.bucketStore = bucketStore;
        this.cacheRef = new AtomicReference<>();
    }

    @Override
    public void startBucketChangeListener() {
        PathChildrenCacheListener bucketListener = (client, event) -> {
            StreamImpl stream;
            switch (event.getType()) {
                case CHILD_ADDED:
                case CHILD_UPDATED:
                    stream = bucketStore.getStreamFromPath(event.getData().getPath());
                    notify(new StreamNotification(stream.getScope(), stream.getStreamName(), NotificationType.StreamAdded));
                    break;
                case CHILD_REMOVED:
                    stream = bucketStore.getStreamFromPath(event.getData().getPath());
                    notify(new StreamNotification(stream.getScope(), stream.getStreamName(), NotificationType.StreamRemoved));
                    break;
                case CONNECTION_LOST:
                    notify(new StreamNotification(null, null, NotificationType.ConnectivityError));
                    break;
                default:
                    log.warn("Received unknown event {} on bucket", event.getType(), getBucketId());
            }
        };
        
        PathChildrenCache pathChildrenCache = cacheRef.updateAndGet(existing -> {
            if (existing == null) {
                PathChildrenCache cache = bucketStore.getBucketPathChildrenCache(getServiceType(), getBucketId());

                cache.getListenable().addListener(bucketListener);
                log.info("bucket {} change notification listener registered", getBucketId());
                return cache;
            } else {
                return existing;
            }
        });

        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        } catch (Exception e) {
            log.error("{}: Starting listener on bucket {} threw exception", getServiceType(), getBucketId(), e);
            throw Exceptions.sneakyThrow(e);
        }
    }

    @Override
    public void stopBucketChangeListener() {
        cacheRef.updateAndGet(cache -> {
            if (cache != null) {
                try {
                    cache.clear();
                    cache.close();
                } catch (IOException e) {
                    log.warn("{}: unable to close watch on bucket {}. Exception thrown ", getServiceType(), getBucketId(), e);
                    throw Exceptions.sneakyThrow(e);
                }
            }
            return null;
        });
    }
}
