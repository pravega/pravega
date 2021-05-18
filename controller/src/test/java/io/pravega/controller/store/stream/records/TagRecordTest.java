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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.*;

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
    public void testSerialization() {
        List<String> streamList = Arrays.asList("stream1", "stream2", "stream3");
        TagRecord r = TagRecord.builder().tagName("t1").streams(new TreeSet<>(streamList)).build();
        byte[] ser = r.toBytes();
        assertEquals(r, TagRecord.fromBytes(ser));
    }

    @Test
    public void testSerializationLength() {
        TreeSet<String> streamSet = new TreeSet<>();
        int length = 0;
        while( length < 8 * 1024 * 1024) {
            List<String> newStreams = new ArrayList<>(50);
            for (int i =0; i < 1000; i ++) {
                String r = RandomStringUtils.random(255, true, true);
                newStreams.add(r);
            }
            streamSet.addAll(newStreams);
            TagRecord rec = TagRecord.builder().tagName("tag1").streams(streamSet).build();
            byte[] ser = rec.toBytes();
            length = ser.length;
            System.out.println("Serialization length " + length + " number of streams " + streamSet.size());
            assertEquals(rec, TagRecord.fromBytes(ser));

        }
    }

    @Test
    public void testStringListCompression() throws IOException {
        List<String> newStreams = new ArrayList<>(50);
        for (int i =0; i < 50; i ++) {
            String r = RandomStringUtils.random(255, true, true);
            newStreams.add(r);
        }
        TreeSet<String> set = new TreeSet<>(newStreams);
        byte[] comp = compressArray(set);
        assertEquals(decompressArray(comp), set);

    }

    public static byte[] compressArray(final TreeSet<String> set) throws IOException {
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        for (String s: set) {
            gzip.write(s.getBytes(StandardCharsets.UTF_8));
            gzip.write(',');
            gzip.flush();
        }
        gzip.close();
        return obj.toByteArray();
    }

    public static Set<String> decompressArray(final byte[] compressed) throws IOException {
        final StringBuilder outStr = new StringBuilder();
        if ((compressed == null) || (compressed.length == 0)) {
            return Collections.emptySet();
        }

        final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            outStr.append(line);
        }
        return Arrays.stream(outStr.toString().split("\\#")).collect(Collectors.toSet());
    }

    public static byte[] compress(final String str) throws IOException {
        if ((str == null) || (str.length() == 0)) {
            return null;
        }
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(str.getBytes("UTF-8"));
        gzip.flush();
        gzip.close();
        return obj.toByteArray();
    }

    public static String decompress(final byte[] compressed) throws IOException {
        final StringBuilder outStr = new StringBuilder();
        if ((compressed == null) || (compressed.length == 0)) {
            return "";
        }

        final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
        final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

        String line;
        while ((line = bufferedReader.readLine()) != null) {
            outStr.append(line);
        }

        return outStr.toString();
    }
}