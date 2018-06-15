/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for FutureReadResultEntryCollection class.
 */
public class FutureReadResultEntryCollectionTests {
    private static final int ENTRY_COUNT = 100;
    private static final int OFFSET_MULTIPLIER = 1000;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the ability to poll entries based on their offsets.
     */
    @Test
    public void testPollEntries() {
        @Cleanup
        FutureReadResultEntryCollection c = new FutureReadResultEntryCollection();
        List<FutureReadResultEntry> entries = generateEntries();
        entries.forEach(c::add);

        FutureReadResultEntry lastEntry = entries.get(entries.size() - 1);
        long maxOffset = lastEntry.getStreamSegmentOffset() + lastEntry.getRequestedReadLength() + OFFSET_MULTIPLIER;
        int skipAmount = (int) (OFFSET_MULTIPLIER * 1.7);
        long previousOffset = -1;
        for (long offset = 0; offset < maxOffset; offset += skipAmount) {
            Collection<FutureReadResultEntry> expectedResult = new ArrayList<>();
            final long checkOffset = offset;
            final long previousCheckOffset = previousOffset;
            entries.forEach(e -> {
                if (e.getStreamSegmentOffset() > previousCheckOffset && e.getStreamSegmentOffset() <= checkOffset) {
                    expectedResult.add(e);
                }
            });

            Collection<FutureReadResultEntry> actualResult = c.poll(offset);
            AssertExtensions.assertContainsSameElements(String.format("Unexpected result from poll(%d).", offset), expectedResult, actualResult, FutureReadResultEntryCollection::entryComparator);

            // Check again, now that we have already removed these entries.
            actualResult = c.poll(offset);
            Assert.assertEquals(String.format("poll(%d) did not remove the entries from the collection.", offset), 0, actualResult.size());
            previousOffset = offset;
        }
    }

    /**
     * Tests the ability for all the pending reads to be canceled when the Collection is closed.
     */
    @Test
    public void testClose() {
        FutureReadResultEntryCollection c = new FutureReadResultEntryCollection();
        List<FutureReadResultEntry> entries = generateEntries();
        entries.forEach(c::add);
        val result = c.close();

        for (FutureReadResultEntry e : entries) {
            Assert.assertFalse("StorageReadResultEntry is completed.", e.getContent().isCancelled());
        }

        AssertExtensions.assertListEquals("Unexpected result from close().", entries, result, Object::equals);
    }

    private List<FutureReadResultEntry> generateEntries() {
        ArrayList<FutureReadResultEntry> entries = new ArrayList<>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            FutureReadResultEntry e = new FutureReadResultEntry(i * OFFSET_MULTIPLIER, OFFSET_MULTIPLIER / 2);
            entries.add(e);
        }

        return entries;
    }
}
