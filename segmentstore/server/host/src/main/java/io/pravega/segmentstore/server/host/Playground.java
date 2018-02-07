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
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.serialization.FormatDescriptor;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import lombok.Builder;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
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
        testSerializer();
    }

    private static void testSerializer() {
        val mc1 = MyClass.builder()
                         .name("name")
                         .id((long) Integer.MAX_VALUE + 123)
                         .nestedClass(new MyNestedClass("myNestedClass"))
                         .tick(12345)
                         .isTrue(true)
                         .build();
        System.out.println("Initial: " + mc1.toString());

        val descriptors = new HashMap<String, FormatDescriptor.WithBuilder<MyClass, MyClass.MyClassBuilder>>();
        descriptors.put("0.2", new MyClassFormat0());
        descriptors.put("1.0", new MyClassFormat1());

        for (val s : descriptors.entrySet()) {
            for (val d : descriptors.entrySet()) {
                System.out.print(String.format("S (%s) -> D(%s): ", s.getKey(), d.getKey()));
                try {
                    @Cleanup
                    val stream = new EnhancedByteArrayOutputStream();
                    val serializer = VersionedSerializer.use(s.getValue());
                    serializer.serialize(mc1, stream);
                    stream.flush();
                    val data = stream.toByteArray();
                    val deserializer = VersionedSerializer.use(d.getValue());
                    val mc2 = deserializer.deserialize(new ByteArrayInputStream(data));
                    System.out.println(mc2);
                } catch (Exception ex) {
                    System.out.println("ERROR");
                    ex.printStackTrace(System.out);
                }
            }
        }
    }

    //region Class (With Builder) and its custom Serializers

    @Builder
    @Getter
    @Setter
    private static class MyClass {
        private final String name;
        private final long id;
        private final MyNestedClass nestedClass;
        private final int tick;
        private final boolean isTrue;

        @Override
        public String toString() {
            return String.format("N=%s, I=%d, T=%d, B=%s NC=%s", this.name, this.id, this.tick, this.isTrue, this.nestedClass);
        }

        static class MyClassBuilder implements ObjectBuilder<MyClass> {
        }
    }

    private static class MyNestedClass {
        private String name;

        MyNestedClass() {
        }

        MyNestedClass(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return "Name=" + this.name;
        }
    }

    private static class MyClassFormat0 extends FormatDescriptor.WithBuilder<MyClass, MyClass.MyClassBuilder> {
        private final VersionedSerializer.Direct<MyNestedClass> ncs00 = VersionedSerializer.use(new MyNestedClassFormat00());
        private final VersionedSerializer.Direct<MyNestedClass> ncs01 = VersionedSerializer.use(new MyNestedClassFormat01());

        @Override
        protected byte writeVersion() {
            return 0;
        }

        @Override
        protected MyClass.MyClassBuilder newBuilder() {
            return MyClass.builder();
        }

        @Override
        protected Collection<FormatVersion<MyClass, MyClass.MyClassBuilder>> getVersions() {
            return Arrays.asList(
                    newVersion(0).revision(0, this::write00, this::read00)
                                 .revision(1, this::write01, this::read01)
                                 .revision(2, this::write02, this::read02),
                    newVersion(1).revision(0, this::write10, this::read10));
        }

        //region Version 0 Revision 0 (Int Id, UTF name)

        private void write00(MyClass source, RevisionDataOutput output) throws IOException {
            output.writeInt((int) Math.min(Integer.MAX_VALUE, source.getId()));
            output.writeUTF(source.getName());
        }

        private void read00(RevisionDataInput input, MyClass.MyClassBuilder targetBuilder) throws IOException {
            targetBuilder.id(input.readInt()); // NOTE: this has been changed to Long in Revision 1
            targetBuilder.name(input.readUTF());
        }

        //endregion

        //region Version 0 Revision 1 (Long Id)

        private void write01(MyClass source, RevisionDataOutput output) throws IOException {
            output.writeLong(source.getId());
        }

        private void read01(RevisionDataInput input, MyClass.MyClassBuilder targetBuilder) throws IOException {
            targetBuilder.id(input.readLong());
        }

        //endregion

        //region Version 0 Revision 2 (+nestedClass, +tick, +isTrue)

        private void write02(MyClass target, RevisionDataOutput output) throws IOException {
            this.ncs01.serialize(target.nestedClass, output);
            output.writeInt(target.getTick());
            output.writeBoolean(target.isTrue());
        }

        private void read02(RevisionDataInput input, MyClass.MyClassBuilder targetBuilder) throws IOException {
            MyNestedClass nc = new MyNestedClass();
            this.ncs00.deserialize(input, nc);
            targetBuilder.nestedClass(nc);
            targetBuilder.tick(input.readInt());
            targetBuilder.isTrue(input.readBoolean());
        }

        //endregion

        //region Version 1 Revision 0 (Long Id, NestedClass, UTF Name)

        private void write10(MyClass target, RevisionDataOutput output) throws IOException {
            output.writeLong(target.getId());
            this.ncs01.serialize(target.nestedClass, output);
            output.writeUTF(target.getName());
            output.writeInt(target.getTick());
            output.writeBoolean(target.isTrue());
        }

        private void read10(RevisionDataInput input, MyClass.MyClassBuilder targetBuilder) throws IOException {
            targetBuilder.id(input.readLong());
            MyNestedClass nc = new MyNestedClass();
            this.ncs00.deserialize(input, nc);
            targetBuilder.nestedClass(nc);
            targetBuilder.name(input.readUTF());
            targetBuilder.tick(input.readInt());
            targetBuilder.isTrue(input.readBoolean());
        }

        //endregion
    }

    private static class MyClassFormat1 extends MyClassFormat0 {
        @Override
        protected final byte writeVersion() {
            return 1;
        }
    }

    private static class MyNestedClassFormat00 extends FormatDescriptor.Direct<MyNestedClass> {
        @Override
        protected byte writeVersion() {
            return 0;
        }

        protected byte writeRevision() {
            return 0;
        }

        @Override
        protected Collection<FormatVersion<MyNestedClass, MyNestedClass>> getVersions() {
            FormatVersion<MyNestedClass, MyNestedClass> v = newVersion(0).revision(0, this::write00, this::read00);
            if (writeRevision() >= 1) {
                v.revision(1, this::write01, this::read01);
            }
            return Collections.singleton(v);
        }

        private void write00(MyNestedClass object, RevisionDataOutput stream) throws IOException {
            stream.writeUTF(object.name);
        }

        private void read00(DataInput s, MyNestedClass b) throws IOException {
            b.name = s.readUTF();
        }

        private void write01(MyNestedClass object, RevisionDataOutput stream) throws IOException {
            stream.writeUTF("This will be ignored.");
        }

        private void read01(DataInput s, MyNestedClass b) throws IOException {
            s.readUTF(); // read dummy written above.
        }
    }

    private static class MyNestedClassFormat01 extends MyNestedClassFormat00 {
        protected byte writeRevision() {
            return 1;
        }
    }

    //endregion
}
