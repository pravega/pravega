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
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.stream.StreamSegments;
import org.apache.commons.lang.NotImplementedException;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ProducerImpl implements ControllerApi.Producer {
    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;

    public ProducerImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(String stream) {
        // fetch active segments from segment store
        return CompletableFuture.supplyAsync(() -> streamStore.getActiveSegments(stream))
                .thenApply(
                        result -> new StreamSegments(result.getCurrent().stream()
                                .map(x -> SegmentHelper.getSegmentId(stream, streamStore.getSegment(stream, x.intValue()), hostStore))
                                .collect(Collectors.toList()), System.currentTimeMillis()));
    }

    @Override
    public CompletableFuture<URI> getURI(SegmentId id) {
        throw new NotImplementedException();
    }
}
