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
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;

/**
 * Base class for InMemory mock implementations.
 * Allows to simulate ChunkStorage with different supported properties.
 */
@Slf4j
abstract public class AbstractInMemoryChunkStorage extends BaseChunkStorage {
    /**
     * value to return when {@link AbstractInMemoryChunkStorage#supportsTruncation()} is called.
     */
    @Getter
    @Setter
    boolean shouldSupportTruncation = true;

    /**
     * value to return when {@link AbstractInMemoryChunkStorage#supportsAppend()} is called.
     */
    @Getter
    @Setter
    boolean shouldSupportAppend = true;

    /**
     * value to return when {@link AbstractInMemoryChunkStorage#supportsConcat()} is called.
     */
    @Getter
    @Setter
    boolean shouldSupportConcat = false;

    @Getter
    @Setter
    long usedSizeToReturn = 0;

    public AbstractInMemoryChunkStorage(Executor executor) {
        super(executor);
    }

    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsTruncation() {
        return shouldSupportTruncation;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsAppend() {
        return shouldSupportAppend;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports merge operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsConcat() {
        return shouldSupportConcat;
    }

    @Override
    protected long doGetUsedSpace(OperationContext opContext) {
        return usedSizeToReturn;
    }

    /**
     * Adds a chunk of given name and length.
     *
     * @param chunkName Name of the chunk to add.
     * @param length Length of the chunk to add.
     */
    abstract public void addChunk(String chunkName, long length);
}
