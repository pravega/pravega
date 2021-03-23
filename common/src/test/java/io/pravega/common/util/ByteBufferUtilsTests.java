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

import java.nio.ByteBuffer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteBufferUtilsTests {

    @Test
    public void testCopy() {
        ByteBuffer a = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
        ByteBuffer b = ByteBuffer.wrap(new byte[] { 10, 11 });
        int num = ByteBufferUtils.copy(a, b);
        assertEquals(2, num);
        assertEquals(2, b.position());
        b.position(0);
        assertEquals(1, b.get());
        assertEquals(2, b.get());
        assertEquals(2, a.position());
        
        a = ByteBuffer.wrap(new byte[] { 1, 2, 3, 4 });
        b = ByteBuffer.wrap(new byte[] { 10, 11 });
        num = ByteBufferUtils.copy(b, a);
        assertEquals(2, num);
        assertEquals(2, a.position());
        assertEquals(4, a.limit());
        assertEquals(2, b.position());
        a.position(0);
        assertEquals(10, a.get());
        assertEquals(11, a.get());
    }

}
