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

import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Unit tests for the VersionedSerializer Class.
 */
public class VersionedSerializerTests {
    //region Test Data

    private static final int COUNT_MULTIPLIER = 4;

    /**
     * We create a set of Tested classes with several layers of nested classes. Based on how many "childrenCount" there are,
     * each of those classes has a few layers of nested classes underneath. Max depth (from top), in this set, is 4 (if
     * childrenCount == COUNT_MULTIPLIER*COUNT_MULTIPLIER, then each of those will have COUNT_MULTIPLIER children,
     * and each of those will have 1 child. This is 3 layers underneath the top-level TestClass objects.
     */
    private static final Collection<TestClass> SINGLE_TYPE_DATA = Arrays.asList(
            TestClass.builder()
                    .name("name1")
                    .id(1)
                    .compactInt(12)
                    .compactLong(13L)
                    .subClasses(immutableClasses(1, 0, 0))
                    .build(),
            TestClass.builder()
                    .name("name2")
                    .id(2)
                    .compactInt(22)
                    .compactLong(23L)
                    .subClasses(immutableClasses(2, 1, COUNT_MULTIPLIER * COUNT_MULTIPLIER))
                    .build(),
            TestClass.builder()
                    .name("name3")
                    .id(3L + Integer.MAX_VALUE)
                    .compactInt(32)
                    .compactLong(33L)
                    .subClasses(immutableClasses(3, COUNT_MULTIPLIER, COUNT_MULTIPLIER))
                    .build(),
            TestClass.builder()
                    .name("name4")
                    .id(4L + Integer.MAX_VALUE)
                    .compactInt(42)
                    .compactLong(43L)
                    .subClasses(immutableClasses(4, COUNT_MULTIPLIER * COUNT_MULTIPLIER, 1))
                    .build()
    );

    private static final List<BaseClass> MULTI_TYPE_DATA = Arrays.asList(
            SubClass1.builder1().id(1).field1(100).build(),
            SubClass2.builder().id(2).field2(200).build(),
            SubClass11.builder11().id(11).field1(1100).build());

    //endregion

    //region Single Type Tests

    /**
     * Tests Single Type serializer using a RandomAccessOutputStream OutputStream (i.e., {@link ByteBufferOutputStream}).
     */
    @Test
    public void testSingleTypeRandomOutput() throws IOException {
        testSingleType((tc, s) -> s.serialize(tc));
    }

    /**
     * Tests Single Type serializer using a Non-Seekable OutputStream.
     */
    @Test
    public void testSingleTypeNonSeekableOutput() throws IOException {
        testSingleType((tc, s) -> {
            val os = new ByteArrayOutputStream();
            s.serialize(os, tc);
            return new ByteArraySegment(os.toByteArray());
        });
    }

    @Test
    public void testThrowingSerializationException() {
        val serializer = new BaseClassSerializer();
        // Setting Mocks
        SubClass1 sc = Mockito.spy(SubClass1.builder1().id(1).field1(100).build());
        OutputStream o = Mockito.mock(OutputStream.class);
        Mockito.doReturn(null).when(sc).getClass();
        // Validating the Serialization Exception.
        Assert.assertThrows(SerializationException.class, () -> serializer.serialize(o, sc));
    }

    /**
     * Tests serialization and deserialization for various cases, including:
     * * Backward and forward compatibility.
     * * Nested classes (multi-layer - see SINGLE_TYPE_DATA above). These are chosen such that in forward compatibility mode,
     * some nested class revisions may be skipped, which verifies that such scenarios work.
     * * Collections and Maps with simple and complex types.
     */
    private void testSingleType(TestClassSerializer serializerFunction) throws IOException {
        // TestClass and ImmutableClass need to have various revisions. Mix and match serializations and deserializations
        // and verify they have been written correctly.
        val descriptors = new HashMap<Integer, VersionedSerializer.Direct<TestClass>>();
        descriptors.put(0, new TestClassSerializer0());
        descriptors.put(1, new TestClassSerializer1());
        descriptors.put(2, new TestClassSerializer2());

        for (val serializer : descriptors.entrySet()) {
            for (val deserializer : descriptors.entrySet()) {
                for (TestClass tc : SINGLE_TYPE_DATA) {
                    // Serialize into the buffer.
                    val data = serializerFunction.apply(tc, serializer.getValue());

                    // Create a blank TestClass and deserialize into it.
                    val tc2 = TestClass.builder().build();
                    deserializer.getValue().deserialize(data, tc2);
                    check(tc, serializer.getKey(), tc2, deserializer.getKey());
                }
            }
        }
    }

    private void check(TestClass source, int sourceRev, TestClass target, int targetRev) {
        String id = String.format("(%d->%d)", sourceRev, targetRev);
        int minRev = Math.min(sourceRev, targetRev);

        Assert.assertEquals("Unexpected TestClass.name for " + id, source.name, target.name);

        if (minRev >= 1) {
            // TestClass.Id was changed from Int to Long.
            Assert.assertEquals("Unexpected TestClass.id for " + id, source.id, target.id);

            // TestClass.subclasses was introduced.
            int sc = source.subClasses == null ? 0 : source.subClasses.size();
            Assert.assertEquals("Unexpected TestClass.subClasses.size() for" + id, sc, target.subClasses.size());
            for (int i = 0; i < sc; i++) {
                check(source.subClasses.get(i), sourceRev, target.subClasses.get(i), targetRev);
            }

        } else {
            Assert.assertEquals("Unexpected TestClass.id(capped) for " + id, Math.min(source.id, Integer.MAX_VALUE), target.id);
            Assert.assertNull("Unexpected TestClass.subClasses for " + id, target.subClasses);
        }

        if (minRev >= 2) {
            // TestClass.compactInt and TestClass.compactLog are introduced in Revision 2.
            Assert.assertEquals("Unexpected TestClass.compactInt for " + id, source.compactInt, target.compactInt);
            Assert.assertEquals("Unexpected TestClass.compactLong for " + id, source.compactLong, target.compactLong);
        } else {
            Assert.assertEquals("Unexpected TestClass.compactInt for " + id, 0, target.compactInt);
            Assert.assertEquals("Unexpected TestClass.compactLong for " + id, 0, target.compactLong);
        }
    }

    private void check(ImmutableClass source, int sourceRev, ImmutableClass target, int targetRev) {
        String id = String.format("(%d->%d)", sourceRev, targetRev);
        int minRev = Math.min(sourceRev, targetRev);
        AssertExtensions.assertGreaterThanOrEqual("TestClass.subClasses was introduced in R1 " + id, 1, minRev);

        Assert.assertEquals("Unexpected ImmutableClass.name for " + id, source.name, target.name);
        Assert.assertEquals("Unexpected ImmutableClass.id for " + id, source.id, target.id);
        if (minRev >= 2) {
            // ImmutableClass.values was introduced (NOTE: minRev refers to TestClass Revision, not ImmutableClass revision).
            int svc = source.values == null ? 0 : source.values.size();
            int tvc = target.values == null ? 0 : target.values.size();
            Assert.assertEquals("Unexpected ImmutableClass.values.size() for" + id, svc, tvc);

            if (svc > 0) {
                for (val se : source.values.entrySet()) {
                    val te = target.values.get(se.getKey());
                    Assert.assertNotNull("ImmutableClass.values is missing element " + se.getKey() + " for " + id, te);
                    check(se.getValue(), sourceRev, te, targetRev);
                }
            }
        }
    }

    private static ArrayList<ImmutableClass> immutableClasses(int seed, int count, int childrenCount) {
        if (count == 0) {
            return null;
        }

        Random rnd = new Random(seed);
        val result = new ArrayList<ImmutableClass>();
        for (int i = 0; i < count; i++) {
            Map<UUID, ImmutableClass> children = null;
            if (childrenCount > 0) {
                AtomicLong j = new AtomicLong(0);
                children = immutableClasses(seed + i, childrenCount, childrenCount / COUNT_MULTIPLIER).stream()
                        .collect(Collectors.toMap(c -> new UUID(j.incrementAndGet(), rnd.nextLong()), c -> c));
            }

            result.add(ImmutableClass.builder()
                    .id(seed + i)
                    .name(String.format("imm_%s_%s", seed, i))
                    .values(children)
                    .build());
        }
        return result;
    }

    //endregion

    //region Multi Type Tests

    /**
     * Tests the ability to serialize and deserialize objects sharing a common base class using a RandomAccessOutputStream OutputStream.
     */
    @Test
    public void testMultiTypeRandomOutput() throws IOException {
        testMultiType(new ByteBufferOutputStream(), ByteBufferOutputStream::getData);
    }

    /**
     * Tests the ability to serialize and deserialize objects sharing a common base class using a non-seekable OutputStream.
     */
    @Test
    public void testMultiTypeNonSeekableOutput() throws IOException {
        testMultiType(new ByteArrayOutputStream(), s -> new ByteArraySegment(s.toByteArray()));
    }

    /**
     * Tests the ability to serialize and deserialize objects sharing a common base class.
     */
    private <T extends OutputStream> void testMultiType(T outStream, Function<T, ByteArraySegment> getData) throws IOException {
        val s = new BaseClassSerializer();

        // Serialize all test data into a single output stream.
        for (val o : MULTI_TYPE_DATA) {
            s.serialize(outStream, o);
        }

        // Deserialize them back and verify contents.
        val inputStream = getData.apply(outStream).getReader();
        for (val expected : MULTI_TYPE_DATA) {
            val deserialized = s.deserialize(inputStream);
            checkMultiType(expected, deserialized);
        }
    }

    private void checkMultiType(BaseClass expected, BaseClass actual) {
        Assert.assertEquals("Unexpected types.", expected.getClass(), actual.getClass());
        if (expected instanceof SubClass11) {
            val e = (SubClass11) expected;
            val a = (SubClass11) actual;
            Assert.assertEquals("Unexpected value for SubClass11.id.", e.id, a.id);
            Assert.assertEquals("Unexpected value for SubClass11.field1.", e.field1, a.field1);
            Assert.assertEquals("Unexpected value for SubClass11.field11.", e.field11, a.field11);
        } else if (expected instanceof SubClass1) {
            val e = (SubClass1) expected;
            val a = (SubClass1) actual;
            Assert.assertEquals("Unexpected value for SubClass1.id.", e.id, a.id);
            Assert.assertEquals("Unexpected value for SubClass1.field1.", e.field1, a.field1);
        } else if (expected instanceof SubClass2) {
            val e = (SubClass2) expected;
            val a = (SubClass2) actual;
            Assert.assertEquals("Unexpected value for SubClass2.id.", e.id, a.id);
            Assert.assertEquals("Unexpected value for SubClass2.field1.", e.field2, a.field2);
        } else {
            Assert.fail("Unexpected type: " + expected.getClass());
        }
    }

    //endregion

    //region Single Type Test Classes and Serializers

    /**
     * This is a mutable class that has some fields and some nested classes. The point of having a Builder is to aid
     * in the test construction.
     */
    @Builder
    private static class TestClass {
        private String name;
        private long id;
        private List<ImmutableClass> subClasses;
        private long compactLong;
        private int compactInt;

        @Override
        public String toString() {
            return String.format("%s, %d, %d, %d, %d", this.name, this.id, this.compactLong, this.compactInt,
                    this.subClasses == null ? 0 : this.subClasses.size());
        }
    }

    /**
     * This is an immutable class that has some nested classes of itself. It provides an ObjectBuilder that aids in
     * construction.
     */
    @Builder
    private static class ImmutableClass {
        private final int id;
        private final String name;
        private final Map<UUID, ImmutableClass> values;

        @Override
        public String toString() {
            return String.format("%d, %s, %d", this.id, this.name, this.values == null ? 0 : this.values.size());
        }

        static class ImmutableClassBuilder implements ObjectBuilder<ImmutableClass> {

        }
    }

    /**
     * In revision 0: we have "name"(String) and "id"(Integer - note this changes to Long in revision 1).
     */
    private static class TestClassSerializer0 extends VersionedSerializer.Direct<TestClass> {
        static final byte VERSION = 0;

        @Override
        protected byte getWriteVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(TestClass source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(Integer.BYTES + output.getUTFLength(source.name));
            }
            output.writeInt((int) Math.min(Integer.MAX_VALUE, source.id));
            output.writeUTF(source.name);
        }

        private void read0(RevisionDataInput input, TestClass target) throws IOException {
            target.id = input.readInt();
            target.name = input.readUTF();
        }
    }

    /**
     * In revision 1: we changed "id" from Integer to Long and added "subClasses".
     */
    private static class TestClassSerializer1 extends TestClassSerializer0 {
        protected final VersionedSerializer.WithBuilder<ImmutableClass, ImmutableClass.ImmutableClassBuilder> ics;

        TestClassSerializer1() {
            this(new ImmutableClassSerializer0());
        }

        protected TestClassSerializer1(VersionedSerializer.WithBuilder<ImmutableClass, ImmutableClass.ImmutableClassBuilder> ics) {
            this.ics = ics;
        }

        @Override
        protected void declareVersions() {
            super.declareVersions();
            version(VERSION).revision(1, this::write1, this::read1);
        }

        private void write1(TestClass source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                int l = Long.BYTES + output.getCollectionLength(source.subClasses, this::getSerializationLength);
                output.length(l);
            }

            output.writeLong(source.id);
            output.writeCollection(source.subClasses, this.ics::serialize);
        }

        private void read1(RevisionDataInput input, TestClass target) throws IOException {
            target.id = input.readLong();
            target.subClasses = input.readCollection(this.ics::deserialize, ArrayList::new);
        }

        @SneakyThrows(IOException.class)
        private int getSerializationLength(ImmutableClass ic) {
            // Note that this is an extremely expensive way to calculate the length. This is appropriate for this unit test
            // but this should be avoided in real production code.
            return this.ics.serialize(ic).getLength();
        }
    }

    /**
     * In revision 2: we added compactLong and compactInt and we are encoding ImmutableClass with a newer revision.
     */
    private static class TestClassSerializer2 extends TestClassSerializer1 {
        TestClassSerializer2() {
            super(new ImmutableClassSerializer1());
        }

        @Override
        protected void declareVersions() {
            super.declareVersions();
            version(VERSION).revision(2, this::write1, this::read1);
        }

        private void write1(TestClass source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(output.getCompactLongLength(source.compactLong) + output.getCompactIntLength(source.compactInt));
            }
            output.writeCompactLong(source.compactLong);
            output.writeCompactInt(source.compactInt);
        }

        private void read1(RevisionDataInput input, TestClass target) throws IOException {
            target.compactLong = input.readCompactLong();
            target.compactInt = input.readCompactInt();
        }
    }

    /**
     * In revision 0, we only encode "id" and "name".
     */
    private static class ImmutableClassSerializer0 extends VersionedSerializer.WithBuilder<ImmutableClass, ImmutableClass.ImmutableClassBuilder> {
        static final byte VERSION = 0;

        @Override
        protected ImmutableClass.ImmutableClassBuilder newBuilder() {
            return ImmutableClass.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(ImmutableClass source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(Integer.BYTES + output.getUTFLength(source.name));
            }
            output.writeInt(source.id);
            output.writeUTF(source.name);
        }

        private void read0(RevisionDataInput input, ImmutableClass.ImmutableClassBuilder target) throws IOException {
            target.id(input.readInt());
            target.name(input.readUTF());
        }
    }

    /**
     * In revision 1, we added the "values" Map.
     */
    private static class ImmutableClassSerializer1 extends ImmutableClassSerializer0 {
        @Override
        protected void declareVersions() {
            super.declareVersions();
            version(VERSION).revision(1, this::write1, this::read1);
        }

        private void write1(ImmutableClass source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(output.getMapLength(source.values, k -> RevisionDataOutput.UUID_BYTES, this::getSerializationLength));
            }
            output.writeMap(source.values, RevisionDataOutput::writeUUID, this::serialize);
        }

        private void read1(RevisionDataInput input, ImmutableClass.ImmutableClassBuilder target) throws IOException {
            target.values(input.readMap(RevisionDataInput::readUUID, this::deserialize));
        }

        @SneakyThrows(IOException.class)
        private int getSerializationLength(ImmutableClass ic) {
            // Note that this is an extremely expensive way to calculate the length. This is appropriate for this unit test
            // but this should be avoided in real production code.
            return serialize(ic).getLength();
        }
    }

    @FunctionalInterface
    interface TestClassSerializer {
        ByteArraySegment apply(TestClass tc, VersionedSerializer.Direct<TestClass> serializer) throws IOException;
    }

    //endregion

    //region Multi Type Classes and Serializers

    @RequiredArgsConstructor
    private static class BaseClass {
        final int id;
    }

    private static class SubClass1 extends BaseClass {
        final int field1;

        @Builder(builderMethodName = "builder1")
        SubClass1(int id, int field1) {
            super(id);
            this.field1 = field1;
        }

        static class SubClass1Builder implements ObjectBuilder<SubClass1> {
        }
    }

    private static class SubClass11 extends SubClass1 {
        final int field11;

        @Builder(builderMethodName = "builder11")
        SubClass11(int id, int field1, int field11) {
            super(id, field1);
            this.field11 = field11;
        }

        static class SubClass11Builder implements ObjectBuilder<SubClass11> {
        }
    }

    private static class SubClass2 extends BaseClass {
        final int field2;

        @Builder
        SubClass2(int id, int field2) {
            super(id);
            this.field2 = field2;
        }

        static class SubClass2Builder implements ObjectBuilder<SubClass2> {
        }
    }

    private static class BaseClassSerializer extends VersionedSerializer.MultiType<BaseClass> {
        @Override
        protected void declareSerializers(Builder b) {
            b.serializer(SubClass1.class, 1, new SubClass1Serializer())
             .serializer(SubClass2.class, 2, new SubClass2Serializer())
             .serializer(SubClass11.class, 11, new SubClass11Serializer());
        }
    }

    private static class SubClass1Serializer extends VersionedSerializer.WithBuilder<SubClass1, SubClass1.SubClass1Builder> {
        static final byte VERSION = 0;

        @Override
        protected SubClass1.SubClass1Builder newBuilder() {
            return SubClass1.builder1();
        }

        @Override
        protected byte getWriteVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(SubClass1 source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(2 * Integer.BYTES);
            }
            output.writeInt(source.id);
            output.writeInt(source.field1);
        }

        private void read0(RevisionDataInput input, SubClass1.SubClass1Builder target) throws IOException {
            target.id(input.readInt());
            target.field1(input.readInt());
        }
    }

    private static class SubClass11Serializer extends VersionedSerializer.WithBuilder<SubClass11, SubClass11.SubClass11Builder> {
        static final byte VERSION = 0;

        @Override
        protected SubClass11.SubClass11Builder newBuilder() {
            return SubClass11.builder11();
        }

        @Override
        protected byte getWriteVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(SubClass11 source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(3 * Integer.BYTES);
            }
            output.writeInt(source.id);
            output.writeInt(source.field1);
            output.writeInt(source.field11);
        }

        private void read0(RevisionDataInput input, SubClass11.SubClass11Builder target) throws IOException {
            target.id(input.readInt());
            target.field1(input.readInt());
            target.field11(input.readInt());
        }
    }

    private static class SubClass2Serializer extends VersionedSerializer.WithBuilder<SubClass2, SubClass2.SubClass2Builder> {
        static final byte VERSION = 0;

        @Override
        protected SubClass2.SubClass2Builder newBuilder() {
            return SubClass2.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(SubClass2 source, RevisionDataOutput output) throws IOException {
            if (output.requiresExplicitLength()) {
                output.length(2 * Integer.BYTES);
            }
            output.writeInt(source.id);
            output.writeInt(source.field2);
        }

        private void read0(RevisionDataInput input, SubClass2.SubClass2Builder target) throws IOException {
            target.id(input.readInt());
            target.field2(input.readInt());
        }
    }

    //endregion
}
