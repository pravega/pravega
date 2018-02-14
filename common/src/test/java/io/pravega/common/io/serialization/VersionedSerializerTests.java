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

import io.pravega.common.ObjectBuilder;
import io.pravega.test.common.AssertExtensions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the VersionedSerializer Class.
 */
public class VersionedSerializerTests {
    private static final int COUNT_MULTIPLIER = 4;

    /**
     * We create a set of Tested classes with several layers of nested classes. Based on how many "childrenCount" there are,
     * each of those classes has a few layers of nested classes underneath. Max depth (from top), in this set, is 4 (if
     * childrenCount == COUNT_MULTIPLIER*COUNT_MULTIPLIER, then each of those will have COUNT_MULTIPLIER children,
     * and each of those will have 1 child. This is 3 layers underneath the top-level TestClass objects.
     */
    private static final Collection<TestClass> TEST_DATA = Arrays.asList(
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

    /**
     * Tests serialization and deserialization for various cases, including:
     * * Backward and forward compatibility.
     * * Nested classes (multi-layer - see TEST_DATA above). These are chosen such that in forward compatibility mode,
     * some nested class revisions may be skipped, which verifies that such scenarios work.
     * * Collections and Maps with simple and complex types.
     */
    @Test
    public void testSerialization() throws IOException {
        // TestClass and ImmutableClass need to have various revisions. Mix and match serializations and deserializations
        // and verify they have been written correctly.
        val descriptors = new HashMap<Integer, VersionedSerializer.Direct<TestClass>>();
        descriptors.put(0, new TestClassSerializer0());
        descriptors.put(1, new TestClassSerializer1());
        descriptors.put(2, new TestClassSerializer2());

        for (val serializer : descriptors.entrySet()) {
            for (val deserializer : descriptors.entrySet()) {
                for (TestClass tc : TEST_DATA) {
                    // Serialize into the buffer.
                    val data = serializer.getValue().serialize(tc);

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

    //region Test Classes

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

    //endregion

    //region TestClass and ImmutableClass Serializers.

    /**
     * In revision 0: we have "name"(String) and "id"(Integer - note this changes to Long in revision 1).
     */
    private static class TestClassSerializer0 extends VersionedSerializer.Direct<TestClass> {
        static final byte VERSION = 0;

        @Override
        protected byte writeVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(TestClass source, RevisionDataOutput output) throws IOException {
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
            output.writeLong(source.id);
            output.writeCollection(source.subClasses, this.ics::serialize);
        }

        private void read1(RevisionDataInput input, TestClass target) throws IOException {
            target.id = input.readLong();
            target.subClasses = input.readCollection(this.ics::deserialize, ArrayList::new);
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
        protected byte writeVersion() {
            return VERSION;
        }

        @Override
        protected void declareVersions() {
            version(VERSION).revision(0, this::write0, this::read0);
        }

        private void write0(ImmutableClass source, RevisionDataOutput output) throws IOException {
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
            output.writeMap(source.values, RevisionDataOutput::writeUUID, this::serialize);
        }

        private void read1(RevisionDataInput input, ImmutableClass.ImmutableClassBuilder target) throws IOException {
            target.values(input.readMap(RevisionDataInput::readUUID, this::deserialize));
        }
    }

    //endregion
}
