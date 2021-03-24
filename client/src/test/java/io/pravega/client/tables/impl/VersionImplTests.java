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

import io.pravega.client.tables.Version;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.apache.commons.lang3.SerializationException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link VersionImpl} class.
 */
public class VersionImplTests {
    @Test
    public void testSpecialVersions() {
        Assert.assertEquals(TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion(), Version.NOT_EXISTS.asImpl().getSegmentVersion());
        Assert.assertEquals(TableSegmentKeyVersion.NO_VERSION.getSegmentVersion(), Version.NO_VERSION.asImpl().getSegmentVersion());
    }

    @Test
    public void testConstructor() {
        long version = 123L;
        long segmentId = 8946L;
        Version v = new VersionImpl(segmentId, version);
        Assert.assertEquals(version, v.asImpl().getSegmentVersion());
    }

    @Test
    public void testFromString() {
        val noSegmentVersion = new VersionImpl(VersionImpl.NO_SEGMENT_ID, 1234L);
        val s1 = Version.fromString(noSegmentVersion.toString()).asImpl();
        Assert.assertEquals(noSegmentVersion, s1);

        val withSegmentVersion = new VersionImpl(123L, 567L);
        val s2 = Version.fromString(withSegmentVersion.toString()).asImpl();
        Assert.assertEquals(withSegmentVersion, s2);

        AssertExtensions.assertThrows(
                "Invalid deserialization worked.",
                () -> Version.fromString("abc"),
                ex -> ex instanceof SerializationException);
    }
}

