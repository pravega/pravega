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
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ExecutorService;

/**
 * Factory for file system Storage adapters.
 */
@RequiredArgsConstructor
public class FileSystemStorageFactory implements StorageFactory {
    @NonNull
    private final FileSystemStorageConfig config;

    @NonNull
    @Getter
    private final ExecutorService executor;

    public Storage createStorageAdapter() {
        FileSystemStorage s = new FileSystemStorage(this.config);
        return new AsyncStorageWrapper(new RollingStorage(s), this.executor);
    }

    @Override
    public SyncStorage createSyncStorage() {
        return new FileSystemStorage(this.config);
    }
}
