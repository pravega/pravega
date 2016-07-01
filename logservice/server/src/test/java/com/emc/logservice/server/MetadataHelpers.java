/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.server;

import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Assert;

import java.util.Collection;

/**
 * Unit test helpers for Metadata tests.
 */
public class MetadataHelpers {

    /**
     * Verify that the given ContainerMetadata objects contain the same data.
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertMetadataEquals(String message, UpdateableContainerMetadata expected, UpdateableContainerMetadata actual) {
        Assert.assertEquals("Unexpected ContainerId.", expected.getContainerId(), actual.getContainerId());
        Collection<Long> expectedSegmentIds = expected.getAllStreamSegmentIds();
        Collection<Long> actualSegmentIds = actual.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements(message + " Unexpected StreamSegments mapped.", expectedSegmentIds, actualSegmentIds);
        for (long streamSegmentId : expectedSegmentIds) {
            SegmentMetadata expectedSegmentMetadata = expected.getStreamSegmentMetadata(streamSegmentId);
            SegmentMetadata actualSegmentMetadata = actual.getStreamSegmentMetadata(streamSegmentId);
            Assert.assertNotNull(message + " No metadata for StreamSegment " + streamSegmentId, actualSegmentMetadata);
            assertSegmentMetadataEquals(message, expectedSegmentMetadata, actualSegmentMetadata);
        }
    }

    /**
     * Verifies that the given SegmentMetadata objects contain the same data.
     * @param message
     * @param expected
     * @param actual
     */
    public static void assertSegmentMetadataEquals(String message, SegmentMetadata expected, SegmentMetadata actual) {
        String idPrefix = message + " SegmentId " + expected.getId();
        Assert.assertEquals(idPrefix + " getId() mismatch.", expected.getId(), actual.getId());
        Assert.assertEquals(idPrefix + " getParentId() mismatch.", expected.getParentId(), actual.getParentId());
        Assert.assertEquals(idPrefix + " getName() isDeleted.", expected.isDeleted(), actual.isDeleted());
        Assert.assertEquals(idPrefix + " getStorageLength() mismatch.", expected.getStorageLength(), actual.getStorageLength());
        Assert.assertEquals(idPrefix + " getDurableLogLength() mismatch.", expected.getDurableLogLength(), actual.getDurableLogLength());
        Assert.assertEquals(idPrefix + " getName() mismatch.", expected.getName(), actual.getName());
        Assert.assertEquals(idPrefix + " isSealed() mismatch.", expected.isSealed(), actual.isSealed());
        Assert.assertEquals(idPrefix + " isMerged() mismatch.", expected.isMerged(), actual.isMerged());
        Assert.assertEquals(idPrefix + " getLastModified() mismatch.", expected.getLastModified(), actual.getLastModified());
    }
}
