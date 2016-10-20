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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * ZK stream metadata store
 */
class ZKStreamMetadataStore extends AbstractStreamMetadataStore {


    private final LoadingCache<String, ZKStream> zkStreams;

    public ZKStreamMetadataStore(StoreConfiguration storeConfiguration) {
        zkStreams = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .removalListener(new RemovalListener<String, ZKStream>() {
                    public void onRemoval(RemovalNotification<String, ZKStream> removal) {
                        removal.getValue().tearDown();
                    }
                })
                .build(
                        new CacheLoader<String, ZKStream>() {
                            public ZKStream load(String key) {
                                ZKStream zkStream = new ZKStream(key, storeConfiguration.getConnectionString());
                                zkStream.init();
                                return zkStream;
                            }
                        });
    }

    @Override
    ZKStream getStream(String name) {
        return zkStreams.getUnchecked(name);
    }

    @Override
    public CompletableFuture<Boolean> createStream(String name, StreamConfiguration configuration, long createTimestamp) {
        return CompletableFuture.supplyAsync(() -> zkStreams.getUnchecked(name)).thenCompose(x -> x.create(configuration, createTimestamp));
    }
}
