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

import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * ZK stream metadata store.
 */
@Slf4j
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore {

    private static final AtomicReference<ZKStreamMetadataStore> SINGLETON = new AtomicReference<>();

    private ZKStreamMetadataStore(ScheduledExecutorService executor) {
        this(ZKUtils.CuratorSingleton.CURATOR_INSTANCE.getCuratorClient(), executor);
    }

    @VisibleForTesting
    public ZKStreamMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        ZKStream.initialize(client, executor);
    }

    @Override
    ZKStream newStream(final String scope, final String name) {
        return new ZKStream(scope, name);
    }

    @Override
    public CompletableFuture<Void> checkpoint(final String id, final String group, final ByteBuffer checkpoint) {
        return ZKStream.checkpoint(id, group, checkpoint);
    }

    @Override
    public CompletableFuture<Optional<ByteBuffer>> readCheckpoint(final String id, final String group) {
        return ZKStream.readCheckpoint(id, group);
    }

    static ZKStreamMetadataStore getSingleton(ScheduledExecutorService executor) {
        if (SINGLETON.get() == null) {
            SINGLETON.compareAndSet(null, new ZKStreamMetadataStore(executor));
        }
        return SINGLETON.get();
    }

}
