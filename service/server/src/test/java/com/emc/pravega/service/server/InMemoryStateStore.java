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

package com.emc.pravega.service.server;

import com.emc.pravega.common.io.EnhancedByteArrayOutputStream;
import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.server.containers.SegmentState;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import lombok.SneakyThrows;
import lombok.val;

/**
 * In-memory mock for AsyncMap.
 */
@ThreadSafe
public class InMemoryStateStore implements AsyncMap<String, SegmentState> {
    private final ConcurrentHashMap<String, ByteArraySegment> map = new ConcurrentHashMap<>();

    @Override
    @SneakyThrows(IOException.class)
    public CompletableFuture<Void> put(String segmentName, SegmentState state, Duration timeout) {
        val innerStream = new EnhancedByteArrayOutputStream();
        val stream = new DataOutputStream(innerStream);
        state.serialize(stream);
        stream.flush();
        this.map.put(segmentName, innerStream.getData());
        return CompletableFuture.completedFuture(null);
    }

    @Override
    @SneakyThrows(IOException.class)
    public CompletableFuture<SegmentState> get(String segmentName, Duration timeout) {
        ByteArraySegment s = this.map.getOrDefault(segmentName, null);
        if (s == null) {
            // No state saved.
            return CompletableFuture.completedFuture(null);
        } else {
            return CompletableFuture.completedFuture(SegmentState.deserialize(new DataInputStream(s.getReader())));
        }
    }

    @Override
    public CompletableFuture<Void> remove(String key, Duration timeout) {
        this.map.remove(key);
        return CompletableFuture.completedFuture(null);
    }
}
