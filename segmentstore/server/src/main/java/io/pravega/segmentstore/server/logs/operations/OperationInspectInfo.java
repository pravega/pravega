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
package io.pravega.segmentstore.server.logs.operations;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OperationInspectInfo extends Operation {
    public static final long DEFAULT_ABSENT_VALUE = Long.MIN_VALUE + 1;
    private String operationTypeString;
    private long length;
    private long segmentId;
    private long offset;
    private long attributes;

    public OperationInspectInfo(long sequenceNumber, String operationTypeString, long length, long segmentId, long offset, long attributes) {
        super();
        setSequenceNumber(sequenceNumber);
        this.operationTypeString = operationTypeString;
        this.length = length;
        this.segmentId = segmentId;
        this.offset = offset;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "{ " + operationTypeString +
                ", SequenceNumber=" + this.getSequenceNumber() +
                (length == DEFAULT_ABSENT_VALUE ? "" : ", Length=" + length) +
                (segmentId == DEFAULT_ABSENT_VALUE ? "" : ", SegmentId=" + segmentId) +
                (offset == DEFAULT_ABSENT_VALUE ? "" : ", Offset=" + offset) +
                (attributes == DEFAULT_ABSENT_VALUE ? "" : ", Attributes=" + attributes) +
                '}';
    }
}
