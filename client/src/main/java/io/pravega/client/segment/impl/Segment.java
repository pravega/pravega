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
package io.pravega.client.segment.impl;

import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.shared.NameUtils;
import java.io.Serializable;
import java.util.List;
import lombok.Data;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
public class Segment implements Comparable<Segment>, Serializable {
    private static final long serialVersionUID = 1L;
    private final String scope;
    @NonNull
    private final String streamName;
    private final long segmentId;
    private transient final String scopedName;

    /**
     * Creates a new instance of Segment class.
     *
     * @param scope      The scope string the segment belongs to.
     * @param streamName The stream name that the segment belongs to.
     * @param id     ID number for the segment.
     */
    public Segment(String scope, String streamName, long id) {
        this.scope = scope;
        this.streamName = streamName;
        this.segmentId = id;
        // Cache the scoped name for performance reasons, as it is checked on the read path.
        this.scopedName = NameUtils.getQualifiedStreamSegmentName(scope, streamName, id);
    }

    public String getScopedStreamName() {
        return NameUtils.getScopedStreamName(scope, streamName);
    }

    public String getScopedName() {
        return NameUtils.getQualifiedStreamSegmentName(scope, streamName, segmentId);
    }

    public String getKVTScopedName() {
        return NameUtils.getQualifiedTableSegmentName(scope, streamName, segmentId);
    }

    public Stream getStream() {
        return new StreamImpl(scope, streamName);
    }

    @Override
    public String toString() {
        return getScopedName();
    }
    
    /**
     * Parses fully scoped name, and creates the segment.
     *
     * @param qualifiedName Fully scoped segment name
     * @return Segment name.
     */
    public static Segment fromScopedName(String qualifiedName) {
        if (NameUtils.isTransactionSegment(qualifiedName)) {
            String originalSegmentName = NameUtils.getParentStreamSegmentName(qualifiedName);
            return fromScopedName(originalSegmentName);
        } else {
            List<String> tokens = NameUtils.extractSegmentTokens(qualifiedName);
            if (tokens.size() == 2) { // scope not present
                String scope = null;
                String streamName = tokens.get(0);
                long segmentId = Long.parseLong(tokens.get(1));
                return new Segment(scope, streamName, segmentId);
            } else { // scope present
                String scope = tokens.get(0);
                String streamName = tokens.get(1);
                long segmentId = Long.parseLong(tokens.get(2));
                return new Segment(scope, streamName, segmentId);
            }
        }
    }
    
    @Override
    public int compareTo(Segment o) {
        int result = scope.compareTo(o.scope);
        if (result == 0) {
            result = streamName.compareTo(o.streamName);
        }
        if (result == 0) {
            result = Long.compare(segmentId, o.segmentId);
        }
        return result;
    }
    
    private Object writeReplace() {
        return new SerializedForm(getScopedName());
    }
    
    @Data
    private static class SerializedForm implements Serializable {
        private static final long serialVersionUID = 1L;
        private final String value;
        Object readResolve() {
            return Segment.fromScopedName(value);
        }
    }
}
