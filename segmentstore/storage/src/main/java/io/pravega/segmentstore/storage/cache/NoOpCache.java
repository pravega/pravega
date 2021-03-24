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
package io.pravega.segmentstore.storage.cache;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.BufferView;
import java.util.function.Supplier;

/**
 * {@link CacheStorage} implementation that does nothing. Only to be used for stubbing.
 */
@VisibleForTesting
public class NoOpCache implements CacheStorage {
    @Override
    public int getBlockAlignment() {
        return 4096;
    }

    @Override
    public int getMaxEntryLength() {
        return CacheLayout.MAX_ENTRY_SIZE;
    }

    @Override
    public void close() {
        // Nothing to do.
    }

    @Override
    public int insert(BufferView data) {
        // Nothing to do.
        return 0;
    }

    @Override
    public int replace(int address, BufferView data) {
        // Nothing to do.
        return address;
    }

    @Override
    public int getAppendableLength(int currentLength) {
        return currentLength == 0 ? getBlockAlignment() : getBlockAlignment() - currentLength % getBlockAlignment();
    }

    @Override
    public int append(int address, int expectedLength, BufferView data) {
        // Nothing to do.
        return Math.min(data.getLength(), getAppendableLength(expectedLength));
    }

    @Override
    public void delete(int address) {
        // Nothing to do.
    }

    @Override
    public BufferView get(int address) {
        return null;
    }

    @Override
    public CacheState getState() {
        return new CacheState(0, 0, 0, 0, CacheLayout.MAX_TOTAL_SIZE);
    }

    @Override
    public void setCacheFullCallback(Supplier<Boolean> cacheFullCallback, int millis) {

    }
}