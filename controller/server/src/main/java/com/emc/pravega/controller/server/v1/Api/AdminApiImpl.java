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

import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.SegmentUri;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang.NotImplementedException;

import java.util.concurrent.CompletableFuture;

public class AdminApiImpl implements ControllerApi.Admin {
    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;

    public AdminApiImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    /***
     * Create the stream metadata in the metadata streamStore.
     * Start with creation of minimum number of segments.
     * Asynchronously call createSegment on pravega hosts about segments in the stream
     */
    @Override
    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        String stream = streamConfig.getName();
        return streamStore.createStream(stream, streamConfig)
                .thenApply(result -> {
                    if (result) {
                        streamStore.getActiveSegments(stream)
                                .thenApply(activeSegments -> {
                                    activeSegments.getCurrent().stream().parallel().forEach(i -> notifyNewSegment(stream, i));
                                    return null;
                                });
                        return Status.SUCCESS;
                    } else {
                        return Status.FAILURE;
                    }
                });
    }

    @Override
    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        throw new NotImplementedException();
    }

    public void notifyNewSegment(String stream, int segmentNumber) {
        // what is previous segment id? There could be multiple previous in case of merge
        SegmentId segmentId = SegmentHelper.getSegment(stream, segmentNumber, -1);
        SegmentUri uri = SegmentHelper.getSegmentUri(stream, segmentId.getNumber(), hostStore);

        // async call, dont wait for its completion or success. Host will contact controller if it does not know
        // about some segment even if this call fails
        CompletableFuture.runAsync(() -> com.emc.pravega.stream.impl.segment.SegmentHelper.createSegment(segmentId.getQualifiedName(), uri));
    }
}
