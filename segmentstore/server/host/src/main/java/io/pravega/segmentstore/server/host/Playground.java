/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Supplier;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.slf4j.LoggerFactory;

/**
 * Playground Test class.
 */
public class Playground {

    public static void main(String[] args) throws Exception {

        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.getLoggerList().get(0).setLevel(Level.INFO);
        //context.reset();

        val mc1 = new MyClass("myname", (long) Integer.MAX_VALUE + 123, 456);
        System.out.println("Initial: " + mc1.toString());

        val serializers = new HashMap<String, Serializer<MyClass>>();
        serializers.put("0.0", new MySerializer00());
        serializers.put("0.1", new MySerializer01());
        serializers.put("0.2", new MySerializer02());
        serializers.put("1.0", new MySerializer10());

        val deserializers = new HashMap<String, Deserializer<MyClass, MyClass.MyClassBuilder>>();
        deserializers.put("0.0", new MyDeserializer00());
        deserializers.put("0.1", new MyDeserializer01());
        deserializers.put("0.2&1.0", new MyDeserializer10());

        for (val s : serializers.entrySet()) {
            for (val d : deserializers.entrySet()) {
                System.out.print(String.format("S (%s) -> D(%s): ", s.getKey(), d.getKey()));
                try {
                    @Cleanup
                    val stream = new ByteArrayOutputStream();
                    s.getValue().serialize(mc1, stream);
                    stream.flush();
                    val data = stream.toByteArray();
                    val mc2 = d.getValue().deserialize(new ByteArrayInputStream(data));
                    System.out.println(mc2);
                } catch (Exception ex) {
                    System.out.println("ERROR");
                    ex.printStackTrace(System.out);
                }
            }
        }
    }

    //region Class (With Builder) and its custom Serializers

    @Data
    @Builder
    private static class MyClass {
        private final String name;
        private final long id;
        private final long tick;

        @Override
        public String toString() {
            return String.format("Name=%s, Id=%d, Tick=%d", this.name, this.id, this.tick);
        }

        static class MyClassBuilder implements ClassBuilder<MyClass> {
        }
    }

    private static class MySerializer00 extends Serializer<MyClass> {
        @Override
        protected final byte version() {
            return 0;
        }

        @Override
        protected void initialize() {
            revision(0, this::serialize0);
        }

        private void serialize0(MyClass object, DataOutputStream stream) throws IOException {
            stream.writeInt((int) Math.min(Integer.MAX_VALUE, object.getId()));
            stream.writeUTF(object.getName());
        }
    }

    private static class MySerializer01 extends MySerializer00 {
        @Override
        protected void initialize() {
            super.initialize();
            revision(1, this::serialize1);
        }

        private void serialize1(MyClass object, DataOutputStream stream) throws IOException {
            stream.writeLong(object.getId());
        }
    }

    private static class MySerializer02 extends MySerializer01 {
        @Override
        protected void initialize() {
            super.initialize();
            revision(2, this::serialize2);
        }

        private void serialize2(MyClass object, DataOutputStream stream) throws IOException {
            stream.writeLong(object.getTick());
        }
    }

    private static class MySerializer10 extends Serializer<MyClass> {
        @Override
        protected final byte version() {
            return 1;
        }

        @Override
        protected void initialize() {
            revision(0, this::serialize0);
        }

        private void serialize0(MyClass object, DataOutputStream stream) throws IOException {
            stream.writeLong(object.getId());
            stream.writeUTF(object.getName());
            stream.writeLong(object.getTick());
        }
    }

    private static class MyDeserializer extends Deserializer<MyClass, MyClass.MyClassBuilder> {
        MyDeserializer() {
            super(MyClass::builder);
        }

        @Override
        protected void initialize() {
            version(0).revision(0, this::version0Revision0)
                      .revision(1, this::version0Revision1)
                      .revision(2, this::version0Revision2);
            version(1).revision(0, this::version1Revision0);
        }

        private void version0Revision0(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.id(s.readInt()); // NOTE: this has been changed to Long in Revision 1
            b.name(s.readUTF());
        }

        private void version0Revision1(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.id(s.readLong());
        }

        private void version0Revision2(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.tick(s.readLong());
        }

        private void version1Revision0(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.id(s.readLong());
            b.name(s.readUTF());
            b.tick(s.readLong());
        }
    }

    // This would go in the first iteration of the code.
    private static class MyDeserializer00 extends Deserializer<MyClass, MyClass.MyClassBuilder> {
        MyDeserializer00() {
            super(MyClass::builder);
        }

        @Override
        protected void initialize() {
            version(0).revision(0, this::version0Revision0);
        }

        private void version0Revision0(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.id(s.readInt()); // NOTE: this has been changed to Long in Revision 1
            b.name(s.readUTF());
        }
    }

    // Second iteration of the code.
    private static class MyDeserializer01 extends MyDeserializer00 {
        @Override
        protected void initialize() {
            super.initialize();
            version(0).revision(1, this::version0Revision1);
        }

        private void version0Revision1(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.id(s.readLong());
        }
    }

    // Third iteration of the code.
    private static class MyDeserializer10 extends MyDeserializer01 {
        @Override
        protected void initialize() {
            super.initialize();
            version(0).revision(2, this::version0Revision2);
            version(1).revision(0, this::version1Revision0);
        }

        private void version0Revision2(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.tick(s.readLong());
        }

        private void version1Revision0(DataInputStream s, MyClass.MyClassBuilder b) throws IOException {
            b.id(s.readLong());
            b.name(s.readUTF());
            b.tick(s.readLong());
        }
    }

    //endregion

    //region Serializer

    private static abstract class Serializer<TClass> {
        private final ArrayList<RevisionSerializer> revisions;

        Serializer() {
            this.revisions = new ArrayList<>();
            initialize();
            Preconditions.checkState(this.revisions.size() < Byte.MAX_VALUE, "Too many revisions. Expected at most %s.", Byte.MAX_VALUE);
        }

        public void serialize(TClass object, OutputStream outputStream) throws IOException {
            // Format (All): V|RCount|Rev1|...|Rev[RCount]
            // Format (Rev): RevId|Data[Variable]
            DataOutputStream stream = outputStream instanceof DataOutputStream ? (DataOutputStream) outputStream : new DataOutputStream(outputStream);

            stream.writeByte(version());
            stream.writeByte(this.revisions.size());
            for (RevisionSerializer rs : this.revisions) {
                stream.writeByte(rs.revision);
                rs.serializer.accept(object, stream);
            }
        }

        protected abstract void initialize();

        protected abstract byte version();

        protected Serializer<TClass> revision(int revision, StreamWriter<TClass> serializer) {
            Preconditions.checkNotNull(serializer, "serializer");
            Preconditions.checkArgument(revision >= 0 && revision <= Byte.MAX_VALUE,
                    "Revision must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
            Preconditions.checkArgument(this.revisions.isEmpty() || revision == this.revisions.get(this.revisions.size() - 1).revision + 1,
                    "Expected revision to be incremental.");
            this.revisions.add(new RevisionSerializer((byte) revision, serializer));
            return this;
        }

        @RequiredArgsConstructor
        private class RevisionSerializer {
            private final byte revision;
            private final StreamWriter<TClass> serializer;
        }

        @FunctionalInterface
        protected interface StreamWriter<TargetType> {
            void accept(TargetType object, DataOutputStream stream) throws IOException;
        }
    }

    //endregion

    //region Deserializer

    public interface ClassBuilder<T> {
        T build();
    }

    private static abstract class Deserializer<TClass, TBuilder extends ClassBuilder<TClass>> {
        private final ArrayList<VersionDeserializer> versions;
        private final Supplier<TBuilder> builder;

        Deserializer(Supplier<TBuilder> builder) {
            this.builder = Preconditions.checkNotNull(builder, "builder");
            this.versions = new ArrayList<>();
            initialize();
        }

        protected abstract void initialize();

        public TClass deserialize(InputStream inputStream) throws IOException {
            // Format (All): V|RCount|Rev1|...|Rev[RCount]
            // Format (Rev): RevId|Data[Variable]

            TBuilder builder = this.builder.get();
            DataInputStream stream = inputStream instanceof DataInputStream ? (DataInputStream) inputStream : new DataInputStream(inputStream);
            byte version = stream.readByte();
            VersionDeserializer vd = this.versions.stream().filter(d -> d.version == version).findFirst().orElse(null);
            checkDataIntegrity(vd != null, "Unsupported version %d.", version);

            byte revisionCount = stream.readByte();
            checkDataIntegrity(revisionCount >= 0, "Data corruption: negative revision count.");

            int revisionIndex = 0;
            for (int i = 0; i < revisionCount; i++) {
                byte revision = stream.readByte();

                RevisionDeserializer rd = vd.getAtIndex(revisionIndex++);
                if (rd == null) {
                    // We've encountered an unknown revision; we cannot read anymore.
                    continue;
                }

                checkDataIntegrity(revision == rd.revision, "Unexpected revision. Expected %d, found %d.", rd.revision, revision);
                rd.deserializer.accept(stream, builder);
            }

            return builder.build();
        }

        private void checkDataIntegrity(boolean condition, String messageFormat, Object... args) throws IOException {
            if (!condition) {
                throw new IOException(String.format(messageFormat, args));
            }
        }

        protected VersionDeserializer version(int version) {
            Preconditions.checkArgument(version >= 0 && version <= Byte.MAX_VALUE,
                    "Version must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
            byte lastVersion = this.versions.isEmpty() ? -1 : this.versions.get(this.versions.size() - 1).version;
            if (version != lastVersion) {
                Preconditions.checkArgument(version == lastVersion + 1, "Expected version to be incremental.");
                VersionDeserializer v = new VersionDeserializer((byte) version);
                this.versions.add(v);
                return v;
            } else {
                return this.versions.get(this.versions.size() - 1);
            }
        }

        protected class VersionDeserializer {
            private final byte version;
            private final ArrayList<RevisionDeserializer> revisions;

            private VersionDeserializer(byte version) {
                this.version = version;
                this.revisions = new ArrayList<>();
            }

            protected VersionDeserializer version(int version) {
                return Deserializer.this.version(version);
            }

            protected VersionDeserializer revision(int revision, StreamReader<TBuilder> deserializer) {
                Preconditions.checkNotNull(deserializer, "deserializer");
                Preconditions.checkArgument(revision >= 0 && revision <= Byte.MAX_VALUE,
                        "Revision must be a non-negative value and less than or equal to %s.", Byte.MAX_VALUE);
                Preconditions.checkArgument(this.revisions.isEmpty() || revision == this.revisions.get(this.revisions.size() - 1).revision + 1,
                        "Expected revision to be incremental.");
                this.revisions.add(new RevisionDeserializer((byte) revision, deserializer));
                return this;
            }

            RevisionDeserializer getAtIndex(int index) {
                return index < this.revisions.size() ? this.revisions.get(index) : null;
            }
        }

        @RequiredArgsConstructor
        private class RevisionDeserializer {
            private final byte revision;
            private final StreamReader<TBuilder> deserializer;
        }

        @FunctionalInterface
        protected interface StreamReader<U> {
            void accept(DataInputStream stream, U builder) throws IOException;
        }
    }

    //endregion
}
