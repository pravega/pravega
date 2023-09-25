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
package io.pravega.segmentstore.server.host.handler;

import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.NameUtils;
import lombok.Data;

/**
 * Index Segment associated with main segment.
 */
@Data
public class IndexEntry {
    private final long offset;
    private final long eventCount;
    private final long timeStamp;

    public BufferView toBytes() {
        ByteArraySegment result = new ByteArraySegment(new byte[NameUtils.INDEX_APPEND_EVENT_SIZE]);
        result.setLong(0, offset);
        result.setLong(8, eventCount);
        result.setLong(16, timeStamp);
        return result;
    }

    public static IndexEntry fromBytes(BufferView view) {
        BufferView.Reader reader = view.getBufferViewReader();
        long offset = reader.readLong();
        long eventCount = reader.readLong();
        long timestamp = reader.readLong();
        return new IndexEntry(offset, eventCount, timestamp);
    }
}
