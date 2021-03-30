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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class InMemorySnapshotInfoStore {

    static private final InMemorySnapshotInfoStore SINGLETON = new InMemorySnapshotInfoStore();

    private final Map<Integer, SystemJournal.SnapshotInfo> data = Collections.synchronizedMap(new HashMap<>());

    public void clear() {
        data.clear();
    }

    public static InMemorySnapshotInfoStore getSingletonInstance() {
        return SINGLETON;
    }

    public SystemJournal.SnapshotInfo getSnapshotId(int containerId) {
        return data.get(containerId);
    }

    public void setSnapshotId(int containerId, SystemJournal.SnapshotInfo checkpoint) {
        data.put(containerId, checkpoint);
    }
}
