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
package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.segment.impl.Segment;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Implementation of {@link SegmentRange}.
 */
@Beta
@Builder
@ToString
@EqualsAndHashCode
public final class SegmentRangeImpl implements SegmentRange {
    private static final long serialVersionUID = 1L;

    /**
     * Segment to which the metadata relates to.
     */
    @NonNull
    @Getter(value = AccessLevel.PUBLIC)
    private final Segment segment;

    /**
     * Start offset for the segment.
     */
    @Getter
    private final long startOffset;

    /**
     * End offset for the segment.
     */
    @Getter
    private final long endOffset;

    @Override
    public long getSegmentId() {
        return segment.getSegmentId();
    }

    @Override
    public String getStreamName() {
        return segment.getStreamName();
    }

    @Override
    public String getScope() {
        return segment.getScope();
    }

    @Override
    public SegmentRangeImpl asImpl() {
        return this;
    }

    public static final class SegmentRangeImplBuilder {
        public SegmentRangeImpl build() {
            Preconditions.checkState(startOffset <= endOffset, "Start offset should be less than end offset.");
            return new SegmentRangeImpl(segment, startOffset, endOffset);
        }
    }
}
