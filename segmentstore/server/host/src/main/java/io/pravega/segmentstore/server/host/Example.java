package io.pravega.segmentstore.server.host;

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.serialization.FormatDescriptor;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.val;

public class Example {
    private static final VersionedSerializer.Direct<Segment> SEGMENT_SERIALIZER = VersionedSerializer.Direct.use(new SegmentFormat());

    private static void main(String[] args) throws Exception {
        val s1 = Segment.builder()
                        .name("segment1").id(1).sealed(true)
                        .transactionIds(Arrays.asList(2L, 3L, 4L))
                        .attributes(new HashMap<>()).build();
        s1.getAttributes().put(1L, new Attribute(10L, 100L));
        s1.getAttributes().put(2L, new Attribute(20L, 200L));

        // Serialize to a Byte Array/OutputStream
        val os = new EnhancedByteArrayOutputStream();
        SEGMENT_SERIALIZER.serialize(os, s1);
        val data = os.getData();

        // Deserialize from an InputStream.
        Segment s2 = Segment.builder().build();
        SEGMENT_SERIALIZER.deserialize(data.getReader(), s2);
    }

    // Segment Class does not have final fields and is expected to be already created upon deserialization.
    @Builder
    @Getter
    @Setter
    private static class Segment {
        private String name;
        private long id;
        private boolean sealed;
        private Collection<Long> transactionIds;
        private Map<Long, Attribute> attributes;
    }

    // Attribute Class has final fields and a Builder - the deserializer will create instances of these.
    @Builder
    @Getter
    private static class Attribute {
        private final Long value;
        private final Long lastUsed;

        static class AttributeBuilder implements ObjectBuilder<Attribute> {
            // Need to extend ObjectBuilder, which simply provides the build() method.
        }
    }

    private static class AttributeFormat extends FormatDescriptor.WithBuilder<Attribute, Attribute.AttributeBuilder> {
        @Override
        protected byte writeVersion() {
            return 0; // Version we're serializing at.
        }

        @Override
        protected Attribute.AttributeBuilder newBuilder() {
            return Attribute.builder();
        }

        @Override
        protected Collection<FormatVersion<Attribute, Attribute.AttributeBuilder>> getVersions() {
            // Attribute has a single version & revision.
            return Collections.singleton(version(0).revision(0, this::write00, this::read00));
        }

        private void write00(Attribute source, RevisionDataOutput output) throws IOException {
            // RevisionDataOutput extends java.io.DataOutput
            output.writeLong(source.getValue());
            output.writeLong(source.getLastUsed());
        }

        private void read00(RevisionDataInput input, Attribute.AttributeBuilder target) throws IOException {
            // RevisionDataInput extends java.io.DataOutput
            target.value(input.readLong());
            target.lastUsed(input.readLong());
        }
    }

    private static class SegmentFormat extends FormatDescriptor.Direct<Segment> {
        private final VersionedSerializer.WithBuilder<Attribute, Attribute.AttributeBuilder> attributeSerializer = VersionedSerializer.use(new AttributeFormat());

        @Override
        protected byte writeVersion() {
            return 0;
        }

        @Override
        protected Collection<FormatVersion<Segment, Segment>> getVersions() {
            //Segment has 2 versions, with version 0 having 3 revisions, and version 1 consolidating those 3 revisions.
            return Arrays.asList(
                    version(0).revision(0, this::write00, this::read00)
                              .revision(1, this::write01, this::read01)
                              .revision(2, this::write02, this::read02),
                    version(1).revision(0, this::write10, this::read10));
        }

        private void write00(Segment source, RevisionDataOutput output) throws IOException {
            output.writeUTF(source.getName());
            output.writeLong(source.getId());
            output.writeBoolean(source.isSealed());
        }

        private void read00(RevisionDataInput input, Segment target) throws IOException {
            target.setName(input.readUTF());
            target.setId(input.readLong());
            target.setSealed(input.readBoolean());
        }

        private void write01(Segment source, RevisionDataOutput output) throws IOException {
            // RevisionDataOutput has built-in APIs to serialize Collections and Maps.
            output.writeCollection(source.getTransactionIds(), RevisionDataOutput::writeLong);
        }

        private void read01(RevisionDataInput input, Segment target) throws IOException {
            // RevisionDataInput has built-in APIs to deserialize Collections and Maps written with RevisionDataOutput.
            target.setTransactionIds(input.readCollection(RevisionDataInput::readLong));
        }

        private void write02(Segment source, RevisionDataOutput output) throws IOException {
            output.writeMap(source.getAttributes(), RevisionDataOutput::writeLong, null);
        }

        private void read02(RevisionDataInput input, Segment target) throws IOException {
            target.setAttributes(input.readMap(RevisionDataInput::readLong, null));
        }

        private void write10(Segment source, RevisionDataOutput output) throws IOException {
            output.writeUTF(source.getName());
            output.writeLong(source.getId());
            output.writeBoolean(source.isSealed());
            output.writeCollection(source.getTransactionIds(), RevisionDataOutput::writeLong);
            output.writeMap(source.getAttributes(), RevisionDataOutput::writeLong, this.attributeSerializer::serialize);
        }

        private void read10(RevisionDataInput input, Segment target) throws IOException {
            target.setName(input.readUTF());
            target.setId(input.readLong());
            target.setSealed(input.readBoolean());
            target.setTransactionIds(input.readCollection(RevisionDataInput::readLong));
            target.setAttributes(input.readMap(RevisionDataInput::readLong, this.attributeSerializer::deserialize));
        }
    }
}
