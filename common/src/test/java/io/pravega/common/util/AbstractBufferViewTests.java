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
package io.pravega.common.util;

import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.test.common.AssertExtensions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HashedArray class.
 */
public class AbstractBufferViewTests {
    private static final int COUNT = 1000;
    private static final int MAX_LENGTH = 100;

    /**
     * Tests equals() and hashCode().
     */
    @Test
    public void testEqualsHashCode() {
        val data1 = generate();
        val data2 = copy(data1);
        BufferView prev = null;
        for (int i = 0; i < data1.size(); i++) {
            val a1 = data1.get(i);
            val a2 = data2.get(i);
            Assert.assertEquals("Expecting hashCode() to be the same for the same array contents.", a1.hashCode(), a2.hashCode());
            Assert.assertTrue("Expecting equals() to return true for the same array contents.", a1.equals(a2) && a2.equals(a1));
            if (prev != null) {
                Assert.assertNotEquals("Expecting hashCode() to be different for different arrays.", prev.hashCode(), a1.hashCode());
                Assert.assertFalse("Expecting equals() to return false for different array contents.", prev.equals(a1) || a1.equals(prev));
            }
            prev = a1;
        }
    }

    /**
     * Tests equals() and hashCode().
     */
    @Test
    public void testEqualsHashCodeComposite() {
        val data = generate();
        val b = data.get(data.size() - 1); // Get the last one.
        val s1 = b.slice(0, b.getLength() / 2);
        val s2 = b.slice(b.getLength() / 2, b.getLength() / 2);
        val cb = BufferView.wrap(Arrays.asList(s1, s2));
        Assert.assertEquals(b.hashCode(), cb.hashCode());
        Assert.assertEquals(b, cb);
        Assert.assertEquals(cb, b);
        val b2Data = b.getCopy();

        // Verify the hashcode stays the same if we make a copy of the buffer.
        Assert.assertEquals(b.hashCode(), (int) BufferView.wrap(b2Data).hash());
        // Verify the hashcode changes if we alter the data.
        b2Data[1] = (byte) (b2Data[1] + 1);
        val b2 = new ByteArraySegment(b2Data);
        Assert.assertNotEquals(b2.hashCode(), cb.hashCode());
        Assert.assertNotEquals(cb, b2);
        Assert.assertNotEquals(b2, cb);
    }

    /**
     * Tests the behavior of the object returned by {@link BufferView#empty()}.
     */
    @Test
    public void testEmptyBufferView() throws Exception {
        val e = BufferView.empty();
        Assert.assertSame("Expecting same instance.", e, BufferView.empty());
        Assert.assertEquals(0, e.getLength());
        Assert.assertEquals(0, e.getAllocatedLength());
        Assert.assertEquals(0, e.getCopy().length);
        Assert.assertSame(e, e.slice(0, 0));
        AssertExtensions.assertThrows("", () -> e.slice(0, 1), ex -> ex instanceof IndexOutOfBoundsException);
        AssertExtensions.assertThrows("", () -> e.getReader(0, 1), ex -> ex instanceof IndexOutOfBoundsException);
        Assert.assertEquals(0, e.copyTo(ByteBuffer.allocate(1)));
        Assert.assertFalse(e.iterateBuffers().hasNext());

        val reader = e.getBufferViewReader();
        Assert.assertEquals(0, reader.available());
        Assert.assertEquals(0, reader.readBytes(ByteBuffer.wrap(new byte[1])));
        AssertExtensions.assertThrows("", reader::readByte, ex -> ex instanceof BufferView.Reader.OutOfBoundsException);
        Assert.assertSame(e, reader.readSlice(0));
        AssertExtensions.assertThrows("", () -> reader.readSlice(1), ex -> ex instanceof BufferView.Reader.OutOfBoundsException);
        @Cleanup
        val inputStream = e.getReader();
        Assert.assertEquals(-1, inputStream.read());
    }

    /**
     * Tests {@link AbstractBufferView.AbstractReader#readInt()}, {@link AbstractBufferView.AbstractReader#readLong()}
     * and {@link AbstractBufferView.AbstractReader#readFully}.
     */
    @Test
    public void testAbstractReader() throws Exception {
        val intValue = 1234;
        val longValue = -123456789L;
        // ReadInt, ReadLong, ReadFully
        val buffer = new ByteArraySegment(new byte[100], 5, 79);
        val rnd = new Random(0);

        // Write some data. Fill with garbage, then put some readable values at the beginning.
        rnd.nextBytes(buffer.array());
        buffer.setInt(0, intValue);
        buffer.setLong(Integer.BYTES, longValue);

        // Now read them back.
        BufferView.Reader reader = buffer.getBufferViewReader();
        Assert.assertEquals(buffer.getLength(), reader.available());

        val readInt = reader.readInt();
        Assert.assertEquals("readInt", intValue, readInt);
        Assert.assertEquals(buffer.getLength() - Integer.BYTES, reader.available());

        val readLong = reader.readLong();
        Assert.assertEquals("readLong", longValue, readLong);
        val remainingDataPos = Integer.BYTES + Long.BYTES;
        val remainingDataLength = buffer.getLength() - remainingDataPos;
        Assert.assertEquals(remainingDataLength, reader.available());

        val remainingData = reader.readFully(3);
        Assert.assertEquals(remainingDataLength, remainingData.getLength());
        Assert.assertEquals("readFully", remainingData, buffer.slice(remainingDataPos, remainingDataLength));
    }

    /**
     * Tests {@link BufferView#builder()}.
     */
    @Test
    public void testBuilder() throws IOException {
        val components = new ArrayList<BufferView>();
        val builder = BufferView.builder();

        // Empty buffer.
        builder.add(BufferView.empty());
        Assert.assertEquals(0, builder.getLength());
        Assert.assertSame("Expected empty buffer view if no components added.", BufferView.empty(), builder.build());

        // One-component buffer.
        val c1 = new ByteArraySegment("component1".getBytes());
        components.add(c1);
        builder.add(c1);
        Assert.assertEquals("Unexpected length with one component.", c1.getLength(), builder.getLength());
        Assert.assertSame("Unexpected result with one component.", c1, builder.build());

        // Multi-component buffer.
        val c2 = new ByteArraySegment("component2".getBytes());
        val c3 = new ByteArraySegment("component3".getBytes());
        val c4 = new ByteArraySegment("component4".getBytes());
        val compositeComponents = Arrays.asList(new BufferView[]{c1, c2, c3, c4}); // Adding same buffer multiple times is OK.
        builder.add(BufferView.wrap(compositeComponents));
        components.addAll(compositeComponents);

        val expectedLength = c1.getLength() * 2 + c2.getLength() + c3.getLength() + c4.getLength();
        Assert.assertEquals(expectedLength, builder.getLength());
        val finalBuffer = builder.build();

        val expectedDataWriter = new ByteBufferOutputStream(expectedLength);
        for (val c : components) {
            c.copyTo(expectedDataWriter);
        }

        val expectedData = expectedDataWriter.getData();
        Assert.assertEquals(expectedData, finalBuffer);
    }

    private List<BufferView> copy(List<BufferView> source) {
        return source.stream()
                .map(b -> new ByteArraySegment(b.getCopy()))
                .collect(Collectors.toList());
    }

    private List<BufferView> generate() {
        final int padding = 10;
        val rnd = new Random(0);
        val result = new ArrayList<BufferView>();
        result.add(BufferView.empty()); // Throw in an empty one too.
        int lastLength = 0;
        for (int i = 0; i < COUNT; i++) {
            int length = (i % 2 == 0 && lastLength > 0) ? lastLength : rnd.nextInt(MAX_LENGTH);
            byte[] array = new byte[length + padding];
            rnd.nextBytes(array);
            int arrayOffset = rnd.nextInt(padding);
            result.add(new ByteArraySegment(array, arrayOffset, length));
            lastLength = length;
        }

        return result;
    }
}
