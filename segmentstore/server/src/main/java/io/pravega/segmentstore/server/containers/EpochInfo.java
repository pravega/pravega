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
package io.pravega.segmentstore.server.containers;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;

import java.io.IOException;

/**
 * Basic info about Container epoch.
 */
@Data
@Builder
public class EpochInfo {
    /**
     * Epoch.
     */
    private final long epoch;
    /**
     * OperationSequenceNumber.
     */
    private final long operationSequenceNumber;

    /**
     * Builder that implements {@link ObjectBuilder}.
     */
    public static class EpochInfoBuilder implements ObjectBuilder<EpochInfo> {
    }

    /**
     * Serializer that implements {@link VersionedSerializer}.
     */
    public static class Serializer extends VersionedSerializer.WithBuilder<EpochInfo, EpochInfoBuilder> {
        @Override
        protected EpochInfo.EpochInfoBuilder newBuilder() {
            return EpochInfo.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(EpochInfo object, RevisionDataOutput output) throws IOException {
            output.writeCompactLong(object.epoch);
            output.writeCompactLong(object.operationSequenceNumber);
        }

        private void read00(RevisionDataInput input, EpochInfo.EpochInfoBuilder b) throws IOException {
            b.epoch(input.readCompactLong());
            b.operationSequenceNumber(input.readCompactLong());
        }
    }
}
