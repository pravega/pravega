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
import java.util.Map;
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

        private void read00(RevisionDataInput revisionDataInput, ContainerSetBuilder ContainerSetBuilder) throws IOException {
            ContainerSetBuilder
                    .containerSet(revisionDataInput.readCollection(DataInput::readInt, HashSet::new));
        }

        private void write00(ContainerSet ContainerSet, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeCollection(ContainerSet.getContainerSet(), DataOutput::writeInt);
        }

        @Override
        protected ContainerSetBuilder newBuilder() {
            return ContainerSet.builder();
        }
    }
}
