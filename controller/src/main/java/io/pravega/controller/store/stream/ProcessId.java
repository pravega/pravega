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

package io.pravega.controller.store.stream;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import lombok.Builder;
import lombok.Getter;

/**
 * ProcessId hold the info of controller host id.
 */
@Builder
public class ProcessId {
    static final ProcessId.ProcessIdSerializer SERIALIZER = new ProcessId.ProcessIdSerializer();

    @Getter
    private final String processId;

    public static class ProcessIdBuilder implements ObjectBuilder<ProcessId> {

    }

    static class ProcessIdSerializer
            extends VersionedSerializer.WithBuilder<ProcessId, ProcessId.ProcessIdBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, ProcessId.ProcessIdBuilder processIdBuilder) throws IOException {
            processIdBuilder
                    .processId(revisionDataInput.readUTF());
        }

        private void write00(ProcessId processId, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(processId.getProcessId());
        }

        @Override
        protected ProcessId.ProcessIdBuilder newBuilder() {
            return ProcessId.builder();
        }
    }
}
