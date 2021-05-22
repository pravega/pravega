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
package io.pravega.controller.store.stream.records;

import org.junit.Test;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class TagRecordTest {

    @Test
    public void testAPI() {
        TagRecord r1 = TagRecord.builder().tagName("t1").stream("s1").stream("s2").build();
        TagRecord r2 = r1.toBuilder().stream("s3").build();
        System.out.println(r1);
        System.out.println(r2);
    }

    @Test
    public void testEmptyStreams() {
        TagRecord r = TagRecord.builder().tagName("t1").build();
        byte[] ser = r.toBytes();
        assertEquals(r, TagRecord.fromBytes(ser));
    }

    @Test
    public void testRemove() {
        TagRecord r = TagRecord.builder().tagName("t1").stream("s1").build();
        TagRecord r1 = r.toBuilder().removeStream("s1").build();
        byte[] ser = r1.toBytes();
        assertEquals(r1, TagRecord.fromBytes(ser));
        assertEquals(0, TagRecord.fromBytes(ser).getStreams().size());
    }

    @Test
    public void testSerialization() {
        List<String> streamList = Arrays.asList("stream1", "stream2", "stream3");
        TagRecord r = TagRecord.builder().tagName("t1").streams(new TreeSet<>(streamList)).build();
        byte[] ser = r.toBytes();
        assertEquals(r, TagRecord.fromBytes(ser));
    }

    //    @Test
    //    public void testSerializationLength() {
    //        TreeSet<String> streamSet = new TreeSet<>();
    //        int length = 0;
    //        String tag = "tag1";
    //        while (length < 1024 * 1024 - 10 * 1024) { // 1MB
    //            List<String> newStreams = new ArrayList<>(50);
    //            for (int i = 0; i < 100; i++) {
    //                newStreams.add(RandomStringUtils.random(255, true, true));
    //            }
    //            streamSet.addAll(newStreams);
    //            TagRecord rec = TagRecord.builder().tagName("tag1").streams(streamSet).build();
    //            Timer timer = new Timer();
    //            byte[] ser = rec.toBytes();
    //            length = ser.length;
    //            assertEquals(rec, TagRecord.fromBytes(ser));
    //            long elapsedNS = timer.getElapsedNanos();
    //            System.out.println("Serialization length " + length + " number of streams " + streamSet.size() + " time " + elapsedNS);
    //        }
    //    }

}