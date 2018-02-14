/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ArrayView;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Custom serializer base class that supports backward and forward compatibility.
 * Refer to VersionedSerializer.Direct and VersionedSerializer.WithBuilder for code samples.
 *
 * @param <TargetType> Type of the object to serialize.
 * @param <ReaderType> Type of the object to deserialize to. This is either TargetType or a shadow object that helps build
 *                     objects of TargetType. The two provided sub-classes have means of choosing this correctly.
 */
public abstract class VersionedSerializer<TargetType, ReaderType> {
    //region Members

    /**
     * The Version of the Serializer format itself.
     * ALL     : FV(1)|V(1)|RevisionCount(1)|Revision1|...|Revision[RevisionCount]
     * REVISION: RevisionId(1)|Length(4)|Data(Length)
     */
    private static final byte SERIALIZER_VERSION = 0;

    // Since Version is a byte, we can use a small array here instead of HashMap which provides better runtime performance.
    private final FormatVersion<TargetType, ReaderType>[] versions;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the VersionedSerializer. Registers all versions and validates the write version is defined
     * correctly.
     */
    @SuppressWarnings("unchecked")
    private VersionedSerializer() {
        this.versions = (FormatVersion<TargetType, ReaderType>[]) new FormatVersion[Byte.MAX_VALUE];
        declareVersions();
        Preconditions.checkArgument(this.versions[writeVersion()] != null, "Write version %s is not defined.", writeVersion());
    }

    //endregion

    //region Version Registration

    /**
     * Gets a value indicating the Version to use for serializing. This will be invoked once during the Constructor (for
     * validation purposes) and once per invocation of serialize().
     */
    protected abstract byte writeVersion();

    /**
     * When implemented in a derived class, this method will declare the FormatVersions that are supported for reading and
     * writing. This method will be invoked once during the constructor.
     */
    protected abstract void declareVersions();

    /**
     * Gets or Creates a FormatVersion with given version. If new, the version will also be registered internally.
     *
     * @param version The version to seek/create.
     * @return A new or existing instance of the FormatVersion class.
     */
    protected FormatVersion<TargetType, ReaderType> version(int version) {
        FormatVersion<TargetType, ReaderType> v = this.versions[version];
        if (v == null) {
            v = new FormatVersion<>(version);
            this.versions[v.getVersion()] = v;
        }

        return v;
    }

    //endregion

    //region Serialization

    /**
     * Serializes the given object to the given RevisionDataOutput. This overload is usually invoked for serializing
     * nested classes or collections.
     *
     * @param dataOutput The RevisionDataOutput to serialize to.
     * @param o          The object to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    public void serialize(RevisionDataOutput dataOutput, TargetType o) throws IOException {
        serialize(dataOutput.getBaseStream(), o);
    }

    /**
     * Serializes the given object to the given OutputStream.
     *
     * @param stream The OutputStream to serialize to.
     * @param o      The object to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    public void serialize(OutputStream stream, TargetType o) throws IOException {
        // Wrap the given stream in a DataOutputStream, but make sure we don't close it, since we don't own it.
        DataOutputStream dataOutput = stream instanceof DataOutputStream ? (DataOutputStream) stream : new DataOutputStream(stream);
        val writeVersion = this.versions[writeVersion()];

        // Write Serialization Header.
        dataOutput.writeByte(SERIALIZER_VERSION);
        dataOutput.writeByte(writeVersion.getVersion());
        dataOutput.writeByte(writeVersion.getRevisions().size());

        // Write each Revision for this Version, in turn.
        for (val r : writeVersion.getRevisions()) {
            dataOutput.writeByte(r.getRevision());
            try (val revisionOutput = RevisionDataOutputStream.wrap(stream)) {
                r.getWriter().accept(o, revisionOutput);
            }
        }
    }

    /**
     * Serializes the given object to an in-memory buffer (RandomOutput) and returns a view of it.
     *
     * @param o The object to serialize.
     * @return An ArrayView which represents the serialized data. This provides a view (offset+length) into a Java byte
     * array and has APIs to extract or copy the data out of there.
     * @throws IOException If an IO Exception occurred.
     */
    public ArrayView serialize(TargetType o) throws IOException {
        val result = new EnhancedByteArrayOutputStream();
        serialize(result, o);
        return result.getData();
    }

    /**
     * Verifies that the given condition is true. If not, throws a SerializationException.
     */
    private void ensureCondition(boolean condition, String messageFormat, Object... args) throws SerializationException {
        if (!condition) {
            throw new SerializationException(String.format(messageFormat, args));
        }
    }

    /**
     * Deserializes data from the given RevisionDataInput into the given object. This overload is usually invoked for
     * deserializing nested classes or collections.
     *
     * @param dataInput The RevisionDataInput to deserialize from.
     * @param target    The target object to apply the deserialization to.
     * @throws IOException If an IO Exception occurred.
     */
    public void deserialize(RevisionDataInput dataInput, ReaderType target) throws IOException {
        deserialize(dataInput.getBaseStream(), target);
    }

    /**
     * Deserializes data from the given InputStream into the given object.
     *
     * @param stream The InputStream to deserialize from.
     * @param target The target object to apply the deserialization to.
     * @throws IOException If an IO Exception occurred.
     */
    public void deserialize(InputStream stream, ReaderType target) throws IOException {
        DataInputStream dataInput = stream instanceof DataInputStream ? (DataInputStream) stream : new DataInputStream(stream);
        byte formatVersion = dataInput.readByte();
        ensureCondition(formatVersion == SERIALIZER_VERSION, "Unsupported format version %d.", formatVersion);
        byte version = dataInput.readByte();
        val readVersion = this.versions[version];
        ensureCondition(readVersion != null, "Unsupported version %d.", version);

        byte revisionCount = dataInput.readByte();
        ensureCondition(revisionCount >= 0, "Data corruption: negative revision count.");

        int revisionIndex = 0;
        for (int i = 0; i < revisionCount; i++) {
            byte revision = dataInput.readByte();
            val rd = readVersion.get(revisionIndex++);
            try (RevisionDataInputStream revisionInput = RevisionDataInputStream.wrap(stream)) {
                if (rd != null) {
                    // We've encountered an unknown revision; we cannot read anymore.
                    ensureCondition(revision == rd.getRevision(),
                            "Unexpected revision. Expected %d, found %d.", rd.getRevision(), revision);
                    rd.getReader().accept(revisionInput, target);
                }
            }
        }
    }

    /**
     * Deserializes data from the given ArrayView into the given object.
     *
     * @param data   The ArrayView to deserialize from.
     * @param target The target object to apply the deserialization to.
     * @throws IOException If an IO Exception occurred.
     */
    public void deserialize(ArrayView data, ReaderType target) throws IOException {
        deserialize(data.getReader(), target);
    }

    //endregion

    //region Versions and Revisions

    /**
     * Represents a Version of a Format. A Version has multiple Revisions that are built incrementally on top of each other,
     * starting with base revision 0 and going up to a maximum revision 127.
     *
     * @param <TargetType> Type of the object to serialize.
     * @param <ReaderType> Type of the object to deserialize to.
     */
    @Getter
    protected static class FormatVersion<TargetType, ReaderType> {
        private final byte version;
        private final ArrayList<FormatRevision<TargetType, ReaderType>> revisions = new ArrayList<>();

        private FormatVersion(int version) {
            Preconditions.checkArgument(version >= 0 && version <= Byte.MAX_VALUE, "Version must be a value between 0 and ", Byte.MAX_VALUE);
            this.version = (byte) version;
        }

        /**
         * Creates a new Revision and registers it.
         *
         * @param revision The Revision Id. If the first revision in the Version, this must be 0. If not, it must be the next
         *                 number higher than the previous Revision Id (revisions ids are incremental numbers).
         * @param writer   A Function that will serialize the contents of the target object into this Revision's Format.
         * @param reader   A Function that will deserialize data pertaining to this Revision's Format into the target object.
         * @return This object.
         */
        public FormatVersion<TargetType, ReaderType> revision(int revision, StreamWriter<TargetType> writer, StreamReader<ReaderType> reader) {
            Preconditions.checkNotNull(writer, "writer");
            Preconditions.checkNotNull(reader, "reader");
            Preconditions.checkArgument(revision >= 0 && revision <= Byte.MAX_VALUE,
                    "Revision must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
            Preconditions.checkArgument(this.revisions.isEmpty() || revision == this.revisions.get(this.revisions.size() - 1).getRevision() + 1,
                    "Expected revision to be incremental.");
            this.revisions.add(new FormatRevision<>((byte) revision, writer, reader));
            return this;
        }

        /**
         * Gets the revision with the given Id, or null if this revision does not exist.
         */
        private FormatRevision<TargetType, ReaderType> get(int revisionId) {
            return revisionId < this.revisions.size() ? this.revisions.get(revisionId) : null;
        }
    }

    /**
     * A FormatRevision, mapping the Revision Id to its Writer and Reader methods.
     */
    @RequiredArgsConstructor
    @Getter
    private static class FormatRevision<TargetType, ReaderType> {
        private final byte revision;
        private final StreamWriter<TargetType> writer;
        private final StreamReader<ReaderType> reader;
    }

    /**
     * Defines a Function that serializes an Object to a RevisionDataOutput.
     *
     * @param <TargetType> The type of the object to serialize.
     */
    @FunctionalInterface
    protected interface StreamWriter<TargetType> {
        void accept(TargetType source, RevisionDataOutput output) throws IOException;
    }

    /**
     * Defines a Function that deserializes data from a RevisionDataInput into a target Object.
     *
     * @param <ReaderType> The type of the object to deserialize into.
     */
    @FunctionalInterface
    protected interface StreamReader<ReaderType> {
        void accept(RevisionDataInput input, ReaderType target) throws IOException;
    }

    //endregion

    //region Types

    /**
     * A VersionedDeserializer that serializes and deserializes into the same object.
     *
     * This should be used in those cases when we already have an instance of the target object available during
     * deserialization and we only need to update its state.
     *
     * Example:
     * * <pre>
     * {@code
     * class Segment { ... }
     *
     * class SegmentSerializer extends VersionedSerializer.Direct<Segment> {
     *    @Override
     *    protected byte writeVersion() { return 0; } // This is the version we'll be serializing now.
     *
     *    @Override
     *    protected void declareVersions() {
     *      // We define all supported versions and their revisions here.
     *      version(0).revision(0, this::write00, this::read00)
     *                .revision(1, this::write01, this::read01);
     *      version(1).revision(0, this::write10, this::read10);
     *    }
     *
     *    // Serializes V0.0; this is the first method to be invoked when serializing V0.
     *    private void write00(Segment source, RevisionDataOutput output) throws IOException { ... }
     *
     *    // Deserializes V0.0 into target; this is the first method to be invoked when deserializing V0.
     *    private void read00(RevisionDataInput input, Segment target) throws IOException { ... }
     *
     *    // Serializes V0.1; this will be invoked after write00() and is an incremental update (only what's new in V0.1).
     *    private void write01(Segment source, RevisionDataOutput output) throws IOException { ... }
     *
     *    // Deserializes V0.1 into target; will be invoked after read00().
     *    private void read01(RevisionDataInput input, Segment target) throws IOException { ... }
     *
     *    // Serializes V1.0; this will be invoked if we ever serialize V1. This will not be mixed with V0.
     *    private void write10(Segment source, RevisionDataOutput output) throws IOException { ... }
     *
     *    // Deserializes V1.0 into target; this will only be invoked for V1.
     *    private void read10(RevisionDataInput input, Segment target) throws IOException { ... }
     * }
     * }
     * </pre>
     *
     * @param <TargetType> Type of the object to serialize from and deserialize into.
     */
    public static abstract class Direct<TargetType> extends VersionedSerializer<TargetType, TargetType> {
    }

    /**
     * A VersionedDeserializer that deserializes into a "Builder" object. A Builder object is a shadow object that accumulates
     * the deserialization changes and is able to create an object of TargetType when the build() method is invoked.
     *
     * This should be used in those cases when we do not have an instance of the target object available during deserialization,
     * most likely because the object is immutable and needs to be created with its data.
     *
     * Example:
     * <pre>
     * {@code
     * class Attribute {
     *    private final Long value;
     *    private final Long lastUsed;
     *
     *    // Attribute class is immutable; it has a builder that helps create new instances (this can be generated with Lombok).
     *    static class AttributeBuilder implements ObjectBuilder<Attribute> { ... }
     * }
     *
     * class AttributeSerializer extends VersionedSerializer.WithBuilder<Attribute, Attribute.AttributeBuilder> {
     *    @Override
     *    protected byte writeVersion() { return 0; } // Version we're serializing at.
     *
     *    @Override
     *    protected Attribute.AttributeBuilder newBuilder() { return Attribute.builder(); }
     *
     *    @Override
     *    protected void declareVersions() {
     *        version(0).revision(0, this::write00, this::read00);
     *    }
     *
     *    private void write00(Attribute source, RevisionDataOutput output) throws IOException { ... }
     *
     *    private void read00(RevisionDataInput input, Attribute.AttributeBuilder target) throws IOException { ... }
     * }
     * }
     * </pre>
     *
     * @param <TargetType> Type of the object to serialize from.
     * @param <ReaderType> Type of the Builder object, which must implement ObjectBuilder(of TargetType). This will be used
     *                     for deserialization.
     */
    public static abstract class WithBuilder<TargetType, ReaderType extends ObjectBuilder<TargetType>> extends VersionedSerializer<TargetType, ReaderType> {
        /**
         * When implemented in a derived class, this method will return a new instance of the Builder each time it is invoked.
         *
         * @return A new instance of ReaderType.
         */
        protected abstract ReaderType newBuilder();

        /**
         * Deserializes data from the given RevisionDataInput and creates a new object with the result. This overload is
         * usually invoked for deserializing nested classes or collections.
         *
         * @param dataInput The RevisionDataInput to deserialize from.
         * @return A new instance of TargetType with the deserialized data.
         * @throws IOException If an IO Exception occurred.
         */
        public TargetType deserialize(RevisionDataInput dataInput) throws IOException {
            return deserialize(dataInput.getBaseStream());
        }

        /**
         * Deserializes data from the given InputStream and creates a new object with the result.
         *
         * @param stream The InputStream to deserialize from.
         * @return A new instance of TargetType with the deserialized data.
         * @throws IOException If an IO Exception occurred.
         */
        public TargetType deserialize(InputStream stream) throws IOException {
            ReaderType builder = newBuilder();
            deserialize(stream, builder);
            return builder.build();
        }

        /**
         * Deserializes data from the given byte array and creates a new object with the result.
         *
         * @param data The byte array to deserialize from.
         * @return A new instance of TargetType with the deserialized data.
         * @throws IOException If an IO Exception occurred.
         */
        public TargetType deserialize(byte[] data) throws IOException {
            return deserialize(new ByteArrayInputStream(data));
        }

        /**
         * Deserializes data from the given ArrayView and creates a new object with the result.
         *
         * @param data The ArrayView to deserialize from.
         * @return A new instance of TargetType with the deserialized data.
         * @throws IOException If an IO Exception occurred.
         */
        public TargetType deserialize(ArrayView data) throws IOException {
            return deserialize(data.getReader());
        }
    }

    //endregion
}
