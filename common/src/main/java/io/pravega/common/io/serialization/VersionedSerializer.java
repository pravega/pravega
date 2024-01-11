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
package io.pravega.common.io.serialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Map;

/**
 * Custom serializer base class that supports backward and forward compatibility.
 *
 * Subclass one of the following based on your needs:
 * * VersionedSerializer.Direct for mutable objects.
 * * VersionedSerializer.WithBuilder for immutable objects with Builders.
 * * VersionedSerializer.MultiType for objects of multiple types inheriting from a common base type.
 *
 * General Notes:
 *
 * Versions
 * * Provide a means of making incompatible format changes, most likely once enough Revisions are accumulated. Serializations
 * in different versions are not meant to be compatible, and that's exactly what the goal of Versions is.
 * * To introduce a new Version B = A + 1, its format needs to be established and published with the code. While doing so,
 * the code must still write version A (since during an upgrade not all existing code will immediately know how to handle
 * Version B). Only after all existing deployed code knows about Version B, can we have the code serialize in Version B.
 * ** This can be achieved by declaring the serialization version using the getWriteVersion() method.
 *
 * Revisions
 * * Are incremental on top of the previous ones, can be added on the fly, and can be used to make format changes
 * without breaking backward or forward compatibility.
 * * Older code will read as many revisions as it knows about, so even if newer code encodes B revisions, older code that
 * only knows about {@literal A < B} revisions will only read the first A revisions, ignoring the rest. Similarly,
 * newer code that knows about B revisions will be able to handle {@literal A < B} revisions by reading
 * as much as is available.
 * ** It is the responsibility of the calling code to fill-in-the-blanks for newly added fields in revisions &gt; A.
 * * Once published, the format for a Version-Revision should never change, otherwise existing (older) code will not be
 * able to process that serialization.
 *
 * OutputStreams/InputStreams
 * * Each Revision's serialization gets an exclusive RevisionDataOutput for writing and an exclusive RevisionDataInput
 * for reading. These are backed by OutputStreams/InputStreams that segment the data within the entire Serialization Stream.
 * * A RevisionDataInput will disallow reading beyond the data serialized for a Revision, and if less data was read, it
 * will skip over the remaining bytes as needed.
 * * A RevisionDataOutput requires the length of the serialization so that it can encode it (for use by RevisionDataInput).
 * This length is encoded as the first 4 bytes of the serialization of each Revision.
 * ** If the target OutputStream (where we serialize to) implements RandomOutput, then the length can be automatically
 * determined and backfilled without any extra work by the caller. Otherwise the caller is required to call length(int)
 * with an appropriate value prior to writing any data to this object so that the length can be written (the serialization
 * will fail if the number of bytes written differs from the length declared).
 * ** RevisionDataOutput has a requiresExplicitLength() to aid in determining which kind of OutputStream is being used.
 * ** RevisionDataOutput has a number of methods that can be used in calculating the length of Strings and other complex
 * structures.
 * ** Consider serializing to a {@link ByteBufferOutputStream} if you want to make use of the RandomOutput features
 * (such as automatic length measurement).
 * *** Consider using {@code ByteArraySegment serialize(T object)} if you want the VersionedSerializer to do this for you.
 * Be mindful that this will create a new buffer for the serialization, which might affect performance.
 *
 * Data Formats
 * * RevisionDataOutput and RevisionDataInput extend Java's DataOutput(Stream) and DataInput(Stream) and they use those
 * classes' implementations for encoding primitive data types.
 * * On top of that, they provide APIs for serializing commonly used structures:
 * ** UUIDs
 * ** Collections
 * ** Maps
 * ** Compact Numbers (Integers which serialize to 1, 2 or 4 bytes and Longs that serialize to 1, 2, 4 or 8 bytes).
 * * Refer to RevisionDataOutput and RevisionDataInput Javadoc for more details.
 *
 * @param <T> Type of the object to serialize.
 */
public abstract class VersionedSerializer<T> {
    //region Serialization primitives

    /**
     * The Version of the Serializer format itself.
     * ALL               : Header|Version(1)|RevisionCount(1)|Revision1|...|Revision[RevisionCount]
     * HEADER(SingleType): SerializerFormatVersion(1)
     * HEADER(MultiType) : SerializerFormatVersion(1)|ObjectType(1)
     * REVISION          : RevisionId(1)|Length(4)|Data(Length)
     * Notes:
     * * SerializerFormatVersion: The version of the Serialization Format itself. This is internal to VersionedSerializer.
     * * Version: The Serialization Version that the caller specifies
     */
    private static final int SERIALIZER_VERSION = 0;

    /**
     * Serializes the given object to the given RevisionDataOutput. This overload is usually invoked for serializing
     * nested classes or collections.
     *
     * @param dataOutput The RevisionDataOutput to serialize to.
     * @param object     The object to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    public void serialize(RevisionDataOutput dataOutput, T object) throws IOException {
        serialize(dataOutput.getBaseStream(), object);
    }

    /**
     * Serializes the given object to an in-memory buffer (RandomAccessOutputStream) and returns a view of it.
     *
     * @param object The object to serialize.
     * @return An ArrayView which represents the serialized data. This provides a view (offset+length) into a Java byte
     * array and has APIs to extract or copy the data out of there.
     * @throws IOException If an IO Exception occurred.
     */
    public ByteArraySegment serialize(T object) throws IOException {
        val result = new ByteBufferOutputStream();
        serialize(result, object);
        return result.getData();
    }

    /**
     * Serializes the given object to the given OutputStream.
     *
     * @param stream The OutputStream to serialize to.
     * @param object The object to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    public abstract void serialize(OutputStream stream, T object) throws IOException;

    /**
     * Reads a single unsigned byte from the given InputStream and interprets it as a Serializer Format Version, after
     * which it validates that it is supported.
     *
     * @param dataInput The InputStream to read from.
     * @throws IOException If an IO Exception occurred.
     */
    void processHeader(InputStream dataInput) throws IOException {
        int formatVersion = dataInput.read();
        if (formatVersion < 0) {
            throw new EOFException();
        }
        ensureCondition(formatVersion == SERIALIZER_VERSION, "Unsupported format version %d.", formatVersion);
    }

    /**
     * Verifies that the given condition is true. If not, throws a SerializationException.
     */
    void ensureCondition(boolean condition, String messageFormat, Object... args) throws SerializationException {
        if (!condition) {
            throw new SerializationException(String.format(messageFormat, args));
        }
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

    //region SingleType

    /**
     * VersionedSerializer base that supports serializing objects of a single type.
     *
     * @param <TargetType> Type of the object to serialize.
     * @param <ReaderType> Type of the object to deserialize.
     */
    private static abstract class SingleType<TargetType, ReaderType> extends VersionedSerializer<TargetType> {
        // Since Version is a byte, we can use a small array here instead of HashMap which provides better runtime performance.
        private final FormatVersion<TargetType, ReaderType>[] versions;

        /**
         * Creates a new instance of the VersionedSerializer. Registers all versions and validates the write version is defined
         * correctly.
         */
        @SuppressWarnings("unchecked")
        SingleType() {
            this.versions = new FormatVersion[Byte.MAX_VALUE];
            declareVersions();
            Preconditions.checkArgument(this.versions[getWriteVersion()] != null, "Write version %s is not defined.", getWriteVersion());
        }

        /**
         * Gets a value indicating the Version to use for serializing. This will be invoked once during the Constructor (for
         * validation purposes) and once per invocation of serialize().
         */
        protected abstract byte getWriteVersion();

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

        @Override
        public void serialize(OutputStream stream, TargetType object) throws IOException {
            beforeSerialization(object);
            stream.write(SERIALIZER_VERSION);
            serializeContents(stream, object);
        }

        /**
         * This method is invoked before any attempt at serializing an object. When implemented in a derived class, it can
         * be used to validate the state of the object and ensure it is ready for serialization.
         *
         * @param object The object that is about to be serialized.
         * @throws IllegalStateException If the given object cannot be serialized at this time.
         */
        protected void beforeSerialization(TargetType object) {
            // This method intentionally left blank in the base class.
        }

        /**
         * Writes the serialization contents, excluding the Header. Refer to the format above for contents.
         *
         * @param stream The OutputStream to write to.
         * @param o      The object to serialize.
         * @throws IOException If an IO Exception occurred.
         */
        void serializeContents(OutputStream stream, TargetType o) throws IOException {
            val writeVersion = this.versions[getWriteVersion()];
            stream.write(writeVersion.getVersion());
            stream.write(writeVersion.getRevisions().size());

            // Write each Revision for this Version, in turn.
            for (val r : writeVersion.getRevisions()) {
                stream.write(r.getRevision());
                try (val revisionOutput = RevisionDataOutputStream.wrap(stream)) {
                    r.getWriter().accept(o, revisionOutput);
                }
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
         * Deserializes data from the given ArrayView into the given object.
         *
         * @param data   The ArrayView to deserialize from.
         * @param target The target object to apply the deserialization to.
         * @throws IOException If an IO Exception occurred.
         */
        public void deserialize(ArrayView data, ReaderType target) throws IOException {
            deserialize(data.getReader(), target);
        }

        /**
         * Deserializes data from the given InputStream into the given object.
         *
         * @param stream The InputStream to deserialize from.
         * @param target The target object to apply the deserialization to.
         * @throws IOException If an IO Exception occurred.
         */
        public void deserialize(InputStream stream, ReaderType target) throws IOException {
            processHeader(stream);
            deserializeContents(stream, target);
        }

        /**
         * Deserializes data from the given InputStream into the given object. This does not attempt to read the serialization
         * header.
         *
         * @param stream The InputStream to deserialize from.
         * @param target The target object to apply the deserialization to.
         * @throws IOException If an IO Exception occurred.
         */
        void deserializeContents(InputStream stream, ReaderType target) throws IOException {
            byte version = readByte(stream);
            val readVersion = this.versions[version];
            ensureCondition(readVersion != null, "Unsupported version %d.", version);

            byte revisionCount = readByte(stream);
            ensureCondition(revisionCount >= 0, "Data corruption: negative revision count.");

            int revisionIndex = 0;
            for (int i = 0; i < revisionCount; i++) {
                byte revision = readByte(stream);
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

        private byte readByte(InputStream in) throws IOException {
            int ch = in.read();
            if (ch < 0) {
                throw new EOFException();
            } else {
                return (byte) ch;
            }
        }
    }

    //endregion

    //region Direct

    /**
     * A Single-Type VersionedDeserializer that serializes and deserializes into the same object.
     *
     * This should be used in those cases when we already have an instance of the target object available during
     * deserialization and we only need to update its state.
     *
     * Example:
     * * <pre>
     * <code>
     * class Segment { ... }
     *
     * class SegmentSerializer extends VersionedSerializer.Direct{@code <Segment>} {
     *    // This is the version we'll be serializing now. We have already introduced read support for Version 1, but
     *    // we cannot write into Version 1 until we know that all deployed code knows how to read it. In order to guarantee
     *    // a successful upgrade when changing Versions, all existing code needs to know how to read the new version.
     *    {@literal @}Override
     *    protected byte getWriteVersion() { return 0; }
     *
     *    {@literal @}Override
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
     * </code></pre>
     *
     * @param <TargetType> Type of the object to serialize from and deserialize into.
     */
    public static abstract class Direct<TargetType> extends SingleType<TargetType, TargetType> {
    }

    //endregion

    //region WithBuilder

    /**
     * A Single-Type VersionedDeserializer that deserializes into a "Builder" object. A Builder object is a shadow object
     * that accumulates the deserialization changes and is able to create an object of TargetType when the build() method
     * is invoked.
     *
     * This should be used in those cases when we do not have an instance of the target object available during deserialization,
     * most likely because the object is immutable and needs to be created with its data.
     *
     * Example:
     * <pre>
     *
     * class Attribute {
     *    private final Long value;
     *    private final Long lastUsed;
     *
     *    // Attribute class is immutable; it has a builder that helps create new instances (this can be generated with Lombok).
     *    static class AttributeBuilder implements ObjectBuilder{@code <Attribute>} { ... }
     * }
     *
     * class AttributeSerializer extends VersionedSerializer.WithBuilder{@code <Attribute, Attribute.AttributeBuilder>} {
     *    {@literal @}Override
     *    protected byte getWriteVersion() { return 0; } // Version we're serializing at.
     *
     *    {@literal @}Override
     *    protected Attribute.AttributeBuilder newBuilder() { return Attribute.builder(); }
     *
     *    {@literal @}Override
     *    protected void declareVersions() {
     *        version(0).revision(0, this::write00, this::read00);
     *    }
     *
     *    private void write00(Attribute source, RevisionDataOutput output) throws IOException { ... }
     *
     *    private void read00(RevisionDataInput input, Attribute.AttributeBuilder target) throws IOException { ... }
     * }
     *
     * </pre>
     *
     * @param <TargetType> Type of the object to serialize from.
     * @param <ReaderType> Type of the Builder object, which must implement ObjectBuilder(of TargetType). This will be used
     *                     for deserialization.
     */
    public static abstract class WithBuilder<TargetType, ReaderType extends ObjectBuilder<TargetType>> extends SingleType<TargetType, ReaderType> {
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
         * @param data The BufferView to deserialize from.
         * @return A new instance of TargetType with the deserialized data.
         * @throws IOException If an IO Exception occurred.
         */
        public TargetType deserialize(BufferView data) throws IOException {
            return deserialize(data.getReader());
        }

        /**
         * Deserializes data from the given InputStream into a new instance of the TargetType type. This does not attempt
         * to read the serialization header; just the contents.
         *
         * @param stream The InputStream to deserialize form.
         * @return A new instance of TargetType.
         * @throws IOException If an IO Exception occurred.
         */
        TargetType deserializeContents(InputStream stream) throws IOException {
            ReaderType builder = newBuilder();
            super.deserializeContents(stream, builder);
            return builder.build();
        }
    }

    //endregion

    //region MultiType

    /**
     * A VersionedDeserializer that serializes deserializes objects instantiating different types that inherit from a single
     * base type. This is a meta-serializer in itself, as it composes various other Single-Type serializers into it.
     *
     * This should be used in those cases when we have a base (maybe abstract) type and multiple types inheriting from it
     * that need serialization. A Serializer needs to be implemented for each sub-type and registered into this instance.
     *
     * Currently only VersionedSerializer.WithBuilder sub-serializers are supported.
     *
     * Example:
     * <pre>
     * <code>
     * class BaseType { ... }
     *
     * class SubType1 extends BaseType {
     *     static class SubType1Builder implements ObjectBuilder{@code <SubType1>} { ... }
     *     static class SubType1Serializer extends VersionedSerializer.WithBuilder{@code <SubType1, SubType1Builder>} { ... }
     * }
     *
     * class SubType11 extends SubType1 {
     *     static class SubType11Builder implements ObjectBuilder{@code <SubType11>} { ... }
     *     static class SubType11Serializer extends VersionedSerializer.WithBuilder{@code <SubType11, SubType11Builder>} { ... }
     * }
     *
     * class SubType2 extends BaseType {
     *     static class SubType2Builder implements ObjectBuilder{@code <SubType2>} { ... }
     *     static class SubType2Serializer extends VersionedSerializer.WithBuilder{@code <SubType2, SubType2Builder>} { ... }
     * }
     *
     * class BaseTypeSerializer extends VersionedSerializer.MultiType{@code <BaseType>} {
     *    {@literal @}Override
     *    protected void declareSerializers(Builder b) {
     *        // Declare sub-serializers here. IDs must be unique, non-changeable (during refactoring) and not necessarily
     *        // sequential or contiguous.
     *        b.serializer(SubType1.class, 0, new SubType1.SubType1Serializer())
     *         .serializer(SubType11.class, 10, new SubType11.SubType11Serializer())
     *         .serializer(SubType2.class, 1, new SubType2.SubType2Serializer());
     *    }
     * }
     * </code></pre>
     *
     * @param <BaseType> The base type that all other types will derive from.
     */
    public static abstract class MultiType<BaseType> extends VersionedSerializer<BaseType> {
        private final Map<Byte, SerializerInfo> serializersById;
        private final Map<Class<?>, SerializerInfo> serializersByType;

        /**
         * Creates a new instance of the MultiType class.
         */
        public MultiType() {
            val builder = new Builder();
            declareSerializers(builder);
            this.serializersByType = builder.builderByType.build();
            this.serializersById = builder.builderById.build();
        }

        /**
         * When implemented in a derived class, this method will declare all supported serializers of subtypes of BaseType
         * by using the serializer() method.
         * @param builder A MultiType.Builder that can be used to declare serializers.
         */
        protected abstract void declareSerializers(Builder builder);

        @Override
        @SuppressWarnings("unchecked")
        public void serialize(OutputStream stream, BaseType o) throws IOException {
            // We need to invoke the beforeSerialization of the target serializer before we actually write anything to
            // the output stream.
            Class c = o.getClass();
            val si = this.serializersByType.get(c);
            if ( si == null ) {
               throw new SerializationException(String.format("No serializer found for %s.", c.getName()));
            }
            si.serializer.beforeSerialization(o);

            // Encode the Serialization Format Version.
            stream.write(SERIALIZER_VERSION);

            // Encode the Object type; this will be used upon deserialization.
            stream.write(si.id);

            // Write contents.
            si.serializer.serializeContents(stream, o);
        }

        /**
         * Deserializes data from the given InputStream into an object of type BaseType. This will use one of the registered
         * serializers to instantiate an instance of the correct type (derived from BaseType), as specified in declareSerializers().
         *
         * @param stream The InputStream to deserialize from.
         * @return The deserialized instance.
         * @throws IOException If an IO Exception occurred.
         */
        @SuppressWarnings("unchecked")
        public BaseType deserialize(InputStream stream) throws IOException {
            processHeader(stream);

            // Decode the object type and look up its serializer.
            byte type = (byte) stream.read();
            if (type < 0) {
                throw new EOFException();
            }

            val si = this.serializersById.get(type);
            if ( si == null ) {
                throw new SerializationException(String.format("No serializer found for %s.", type));
            }
            return (BaseType) si.serializer.deserializeContents(stream);
        }

        /**
         * Deserializes data from the given ArrayView and creates a new object with the result.
         *
         * @param data The ArrayView to deserialize from.
         * @return A new instance of TargetType with the deserialized data.
         * @throws IOException If an IO Exception occurred.
         */
        public BaseType deserialize(ArrayView data) throws IOException {
            return deserialize(data.getReader());
        }
        
        @RequiredArgsConstructor
        private static class SerializerInfo {
            final Class<?> type;
            final byte id;
            final VersionedSerializer.WithBuilder serializer;
        }

        protected final class Builder {
            private final ImmutableMap.Builder<Byte, SerializerInfo> builderById = ImmutableMap.builder();
            private final ImmutableMap.Builder<Class<?>, SerializerInfo> builderByType = ImmutableMap.builder();

            /**
             * Registers a new serializer for the given class.
             *
             * @param type                The type of the class to register. Must derive from BaseClass.
             * @param serializationTypeId A unique identifier associated with this serializer. This will be used to identify
             *                            object types upon deserialization, so it is very important for this value not to
             *                            change or be reused upon code refactoring. Valid range: [0, 127]
             * @param serializer          The serializer for the given type.
             * @param <TargetType>        Type of the object to serialize. Must derive from BaseType.
             * @param <ReaderType>        A type implementing ObjectBuilder(of TargetType) that can be used to create new objects.
             * @return This instance.
             */
            public <TargetType extends BaseType, ReaderType extends ObjectBuilder<TargetType>> MultiType<BaseType>.Builder serializer(
                    Class<TargetType> type, int serializationTypeId, VersionedSerializer.WithBuilder<TargetType, ReaderType> serializer) {
                Preconditions.checkArgument(serializationTypeId >= 0 && serializationTypeId <= Byte.MAX_VALUE,
                        "SerializationTypeId must be a value between 0 and ", Byte.MAX_VALUE);

                val si = new SerializerInfo(type, (byte) serializationTypeId, serializer);
                this.builderById.put(si.id, si);
                this.builderByType.put(si.type, si);
                return this;
            }
        }
    }

    //endregion
}
