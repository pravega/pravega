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

package io.pravega.test.integration.selftest;

import io.pravega.test.integration.selftest.adapters.StoreAdapter;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Base Data Source for a Self Test Producer.
 * @param <T> Type of update to add to Targets.
 */
@RequiredArgsConstructor
abstract class ProducerDataSource<T extends ProducerUpdate> implements AutoCloseable {
    protected static final String LOG_ID = "DataSource";
    @NonNull
    protected final TestConfig config;
    @NonNull
    protected final TestState state;
    @NonNull
    protected final StoreAdapter store;

    @Override
    public void close() {
        // Nothing to do here. Derived classes may have different implementations but we do not force this upon them.
    }

    /**
     * Generates a new {@link ProducerOperation} that can be executed next.
     * @param producerId Id of the {@link Producer} to generate for.
     * @return A new {@link ProducerOperation}.
     */
    abstract ProducerOperation<T> nextOperation(int producerId);

    /**
     * Creates all Targets (as defined by the actual implementation of this class).
     *
     * @return A CompletableFuture that, when complete, will indicate all targets have been created.
     */
    abstract CompletableFuture<Void> createAll();

    /**
     * Deletes all Targets (as defined by the actual implementation of this class)
     *
     * @return A CompletableFuture that, when complete, will indicate all targets have been deleted.
     */
    abstract CompletableFuture<Void> deleteAll();

    /**
     * Determines if the Target (Stream/Table) with given name is closed for modifications or not.
     */
    boolean isClosed(String name) {
        TestState.StreamInfo si = this.state.getStream(name);
        return si == null || si.isClosed();
    }

    /**
     * Exception that is thrown whenever an unknown Stream/Segment name is passed to this data source (one that was not
     * created using it).
     */
    static class UnknownTargetException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        UnknownTargetException(String segmentName) {
            super(String.format("No such Stream/Segment was created using this DataSource: %s.", segmentName));
        }
    }
}
