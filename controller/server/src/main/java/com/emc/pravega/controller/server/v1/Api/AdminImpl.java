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
package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.common.hash.ConsistentHash;
import com.emc.pravega.controller.contract.v1.api.Api;
import com.emc.pravega.controller.store.host.Host;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

public class AdminImpl implements Api.Admin {
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;

    public AdminImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    @Override
    /***
     * Create the stream metadata in the metadata streamStore.
     * Start with creation of minimum number of segments.
     * Asynchronously notify all pravega hosts about segments in the stream
     */
    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        String stream = streamConfig.getName();
        return CompletableFuture.supplyAsync(() -> streamStore.createStream(stream, streamConfig))
                .thenApply(result -> {
                    if (result) {
                        for (int i = 0; i < streamConfig.getScalingingPolicy().getMinNumSegments(); i++) {
                            // create segments, compute their hashes
                            // TODO: Figure out how to define key ranges
                            Segment segment = new Segment(i, 0, Long.MAX_VALUE, 0.0, 0.0);
                            streamStore.addActiveSegment(stream, segment);
                            int container = ConsistentHash.hash(stream + segment.getNumber(), hostStore.getContainerCount());
                            Host host = hostStore.getHostForContainer(container);
                            // TODO: make asynchronous and non blocking call into host to inform it about new segment ownership.
                        }
                        return Status.SUCCESS;
                    } else return Status.FAILURE;
                });
    }

    @Override
    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        throw new NotImplementedException();
    }
}
