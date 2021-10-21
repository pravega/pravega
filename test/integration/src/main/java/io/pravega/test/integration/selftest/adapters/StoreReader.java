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
package io.pravega.test.integration.selftest.adapters;

import io.pravega.common.concurrent.CancellationToken;
import io.pravega.test.integration.selftest.Event;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a general Reader that can be used to access data within a StoreAdapter.
 */
public interface StoreReader {

    /**
     * Reads the entire Target (Stream/Segment) from the beginning, one Event at a time. When (if) the read catches up
     * to the end of the current Target, this will turn into a tail-reader, unless the Target is Sealed.
     *
     * @param target            The Target (Stream/Segment) to read.
     * @param eventHandler      A callback that will be invoked on each ReadItem read.
     * @param cancellationToken A CancellationToken that can be used to cancel the read operation.
     * @return A CompletableFuture that, when completed normally, indicates that the entire Target has been read. If this
     * Future completes exceptionally, it will indicate that an unrecoverable error occurred while reading, or the read
     * was interrupted.
     */
    CompletableFuture<Void> readAll(String target, java.util.function.Consumer<ReadItem> eventHandler, CancellationToken cancellationToken);

    /**
     * Reads exactly one item at the specified address.
     *
     * @param target  The Target (Stream/Segment) to read from.
     * @param address The address to read at. This can be obtained by invoking ReadItem.getAddress() on returned items
     *                from readAll().
     * @return A CompletableFuture that, when completed normally, will contain a ReadItem with the read Event.
     */
    CompletableFuture<ReadItem> readExact(String target, Object address);

    /**
     * Reads the entire Storage data associated with the given target.
     *
     * @param target  The Target (Stream/Segment) to read from.
     * @param eventHandler      A callback that will be invoked on each Event read.
     * @param cancellationToken A CancellationToken that can be used to cancel the read operation.
     * @return A CompletableFuture that, when completed normally, indicates that the entire Target has been read. If this
     * Future completes exceptionally, it will indicate that an unrecoverable error occurred while reading, or the read
     * was interrupted.
     */
    CompletableFuture<Void> readAllStorage(String target, java.util.function.Consumer<Event> eventHandler, CancellationToken cancellationToken);

    /**
     * Defines an item that is read.
     */
    interface ReadItem {
        /**
         * The Event that was read.
         */
        Event getEvent();

        /**
         * The address of the Event.
         */
        Object getAddress();
    }
}
