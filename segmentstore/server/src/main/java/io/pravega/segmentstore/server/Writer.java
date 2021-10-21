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
package io.pravega.segmentstore.server;

import com.google.common.util.concurrent.Service;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a component that pulls data from an OperationLog and writes it to a Storage. This is a background service that
 * does not expose any APIs, except for those controlling its lifecycle.
 */
public interface Writer extends Service, AutoCloseable {
    @Override
    void close();

    /**
     * Forces a "flush" of all Segment Operations up to at least the given Sequence Number.
     *
     * @param upToSequenceNumber The Sequence Number up to which to flush. Note that a flush may include Operations
     *                           with Sequence Numbers exceeding this one.
     * @param timeout            Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the outcome of the operation. If anything was flushed,
     * it will be completed with {@link Boolean#TRUE}, otherwise it will be completed with {@link Boolean#FALSE}.
     */
    CompletableFuture<Boolean> forceFlush(long upToSequenceNumber, Duration timeout);
}
