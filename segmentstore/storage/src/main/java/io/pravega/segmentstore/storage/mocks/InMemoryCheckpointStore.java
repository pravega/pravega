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

import io.pravega.segmentstore.storage.chunklayer.SystemJournal;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.val;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InMemoryCheckpointStore implements SystemJournal.CheckpointStore {

    static private final InMemoryCheckpointStore SINGLETON = new InMemoryCheckpointStore();

    private final Map<String, VersionedData> data = Collections.synchronizedMap(new HashMap<>());

    public synchronized byte[] getData(String path, Long version) throws Exception {
        val entry = data.get(path);
        if (null != entry) {
            return entry.getValue();
        }
        return null;
    }

    public synchronized Long setData(String path, byte[] value, Long version) throws Exception {
        if (null == version) {
            data.put(path, new VersionedData(value, 0L));
            return 0L;
        } else {
            val entry = data.get(path);
            if (null != entry) {
                if (!entry.version.equals(version)) {
                    throw new Exception("Mismatch");
                }
                data.put(path, new VersionedData(value, entry.version + 1));
                return entry.version + 1;
            }
        }
        throw new Exception("Mismatch");
    }

    public void clear() {
        data.clear();
    }

    public static InMemoryCheckpointStore getSingletonInstance() {
        return SINGLETON;
    }

    @Data
    @AllArgsConstructor
    static class VersionedData {
        byte[] value;
        Long version;
    }
}
