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
package io.pravega.client.tables.impl;

import io.netty.buffer.Unpooled;
import java.util.Random;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the {@link TableSegmentKey} class.
 */
public class TableSegmentKeyTests {
    private static final int KEY_LENGTH = 123;
    private byte[] keyData;

    @Before
    public void setup() {
        val rnd = new Random(0);
        this.keyData = new byte[KEY_LENGTH];
        rnd.nextBytes(this.keyData);
    }

    @Test
    public void testUnversioned() {
        val e = TableSegmentKey.unversioned(this.keyData);
        check(e, TableSegmentKeyVersion.NO_VERSION.getSegmentVersion());
    }

    @Test
    public void testNotExists() {
        val e = TableSegmentKey.notExists(this.keyData);
        check(e, TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion());
    }

    @Test
    public void testVersioned() {
        val e = TableSegmentKey.versioned(this.keyData, 123);
        check(e, 123);
    }

    private void check(TableSegmentKey e, long expectedVersion) {
        Assert.assertEquals(expectedVersion, e.getVersion().getSegmentVersion());
        Assert.assertEquals(Unpooled.wrappedBuffer(this.keyData), e.getKey());
    }
}
