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
package io.pravega.common.cluster;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@Builder
public class ContainerSet {
    static final ContainerSetSerializer SERIALIZER = new ContainerSetSerializer();

    @Getter
    private final Set<Integer> containerSet;
    
    public static class ContainerSetBuilder implements ObjectBuilder<ContainerSet> {

    }

    static class ContainerSetSerializer
            extends VersionedSerializer.WithBuilder<ContainerSet, ContainerSetBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, ContainerSetBuilder containerSetBuilder) throws IOException {
            containerSetBuilder
                    .containerSet(revisionDataInput.readCollection(DataInput::readInt, HashSet::new));
        }

        private void write00(ContainerSet containerSet, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCollection(containerSet.getContainerSet(), DataOutput::writeInt);
        }

        @Override
        protected ContainerSetBuilder newBuilder() {
            return ContainerSet.builder();
        }
    }
}
