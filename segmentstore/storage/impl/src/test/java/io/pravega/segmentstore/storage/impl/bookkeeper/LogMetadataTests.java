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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the LogMetadata class.
 */
public class LogMetadataTests {
    private static final long LEDGER_COUNT = 10;

    /**
     * Tests addLedger(), truncate(), getLedger().
     */
    @Test(timeout = 5000)
    public void testLedgerOperations() {
        Supplier<Long> nextLedgerId = new AtomicLong()::incrementAndGet;
        LogMetadata metadata = null;
        long expectedEpoch = 0;
        val expectedLedgerIds = new ArrayList<Long>();
        for (int i = 0; i < LEDGER_COUNT; i++) {
            expectedEpoch++;
            long ledgerId = nextLedgerId.get() * 2;
            expectedLedgerIds.add(ledgerId);
            if (metadata == null) {
                metadata = new LogMetadata(ledgerId).withUpdateVersion(i);
            } else {
                metadata.withUpdateVersion(i);
                metadata = metadata.addLedger(ledgerId);
            }

            Assert.assertEquals("Unexpected epoch.", expectedEpoch, metadata.getEpoch());
            Assert.assertEquals("Unexpected update version.", i, metadata.getUpdateVersion());
            checkLedgerIds(expectedLedgerIds, metadata);
        }

        // Verify that sequence numbers are assigned incrementally.
        for (int i = 1; i < metadata.getLedgers().size(); i++) {
            Assert.assertEquals("Sequence is not incremented.",
                    metadata.getLedgers().get(i - 1).getSequence() + 1,
                    metadata.getLedgers().get(i).getSequence());
        }

        // Test the getLedger method.
        long maxLedgerId = expectedLedgerIds.get(expectedLedgerIds.size() - 1);
        for (long ledgerId = 0; ledgerId < maxLedgerId; ledgerId++) {
            val lm = metadata.getLedger(ledgerId);
            if (expectedLedgerIds.contains(ledgerId)) {
                Assert.assertNotNull("Existing LedgerMetadata was not found.", lm);
                Assert.assertEquals("Unexpected ledger id returned.", ledgerId, lm.getLedgerId());
            } else {
                Assert.assertNull("A LedgerMetadata that did not exist was returned.", lm);
            }
        }

        // Test truncate.
        val truncatedLedgerIds = new ArrayList<Long>();
        for (long ledgerId : expectedLedgerIds) {
            val truncatedMetadata = metadata.truncate(new LedgerAddress(0, ledgerId, 123));

            // Verify truncated metadata only contains those ledger ids that haven't been truncated out.
            val expectedRemainingLedgerIds = expectedLedgerIds.stream().filter(o -> !truncatedLedgerIds.contains(o)).collect(Collectors.toList());
            checkLedgerIds(expectedRemainingLedgerIds, truncatedMetadata);

            // Verify original metadata contains all ledger ids.
            checkLedgerIds(expectedLedgerIds, metadata);

            // Only add this after we did all checks, since the currently truncated ledger is still active.
            truncatedLedgerIds.add(ledgerId);
        }

        // Test markEmptyLedgers.
        val lacs = expectedLedgerIds.stream()
                .filter(i -> i % 3 > 0)
                .collect(Collectors.toMap(i -> i, i -> i % 3 == 1 ? Ledgers.NO_ENTRY_ID : 1000));
        val m = metadata.updateLedgerStatus(lacs);
        for (long ledgerId : expectedLedgerIds) {
            Assert.assertEquals("markEmptyLedgers modified base metadata",
                    LedgerMetadata.Status.Unknown, metadata.getLedger(ledgerId).getStatus());
            long lac = lacs.getOrDefault(ledgerId, Long.MIN_VALUE);
            LedgerMetadata.Status expectedStatus = lac == Long.MIN_VALUE
                    ? LedgerMetadata.Status.Unknown
                    : (lac == -1 ? LedgerMetadata.Status.Empty : LedgerMetadata.Status.NotEmpty);
            Assert.assertEquals("markEmptyLedgers did not return an updated metadata.",
                    expectedStatus, m.getLedger(ledgerId).getStatus());
        }

        // Test removeEmptyLedgers.
        final int skipCount = 3;
        val m2 = m.removeEmptyLedgers(skipCount);
        checkLedgerIds(expectedLedgerIds, m);
        expectedLedgerIds.clear();
        // We rebuild expectedLedgerIds based on the previous metadata. We filter out the Empty Ledgers and add the ones
        // we were asked for from the end.
        m.getLedgers().stream()
                .filter(lm -> lm.getStatus() != LedgerMetadata.Status.Empty)
                .map(LedgerMetadata::getLedgerId)
                .forEach(expectedLedgerIds::add);
        for (int i = m.getLedgers().size() - skipCount; i < m.getLedgers().size(); i++) {
            long ledgerId = m.getLedgers().get(i).getLedgerId();
            if (!expectedLedgerIds.contains(ledgerId)) {
                expectedLedgerIds.add(ledgerId);
            }
        }

        expectedLedgerIds.sort(Long::compare); // The expected list may not be sorted; do it now.
        checkLedgerIds(expectedLedgerIds, m2);
    }

    /**
     * Tests the getNextAddress() method.
     */
    @Test(timeout = 5000)
    public void testGetNextAddress() {
        // No ledgers.
        val empty = new LogMetadata(1).truncate(new LedgerAddress(2, 2, 2));
        Assert.assertNull("Unexpected result from empty metadata",
                empty.getNextAddress(new LedgerAddress(1, 1, 0), Long.MAX_VALUE));

        Supplier<Long> nextLedgerId = new AtomicLong()::incrementAndGet;
        LogMetadata m = null;
        for (int i = 0; i < LEDGER_COUNT; i++) {
            long ledgerId = nextLedgerId.get() * 2;
            if (m == null) {
                m = new LogMetadata(ledgerId).withUpdateVersion(i);
            } else {
                m.withUpdateVersion(i);
                m = m.addLedger(ledgerId);
            }
        }

        // Address less than first ledger.
        val firstLedgerId = m.getLedgers().get(0).getLedgerId();
        val firstLedgerSeq = m.getLedgers().get(0).getSequence();
        LedgerAddress a = m.getNextAddress(new LedgerAddress(firstLedgerSeq - 1, firstLedgerId - 1, 1234), Long.MAX_VALUE);
        Assert.assertEquals("Unexpected ledger id when input address less than first ledger.", firstLedgerId, a.getLedgerId());
        Assert.assertEquals("Unexpected entry id when input address less than first ledger.", 0, a.getEntryId());

        // Address less than truncation address.
        val secondLedgerId = m.getLedgers().get(1).getLedgerId();
        val secondLedgerSeq = m.getLedgers().get(1).getSequence();
        LedgerAddress truncationAddress = new LedgerAddress(secondLedgerSeq, secondLedgerId, 1);
        val m2 = m.truncate(truncationAddress);
        a = m2.getNextAddress(new LedgerAddress(firstLedgerSeq, firstLedgerId, 0), Long.MAX_VALUE);
        Assert.assertEquals("Unexpected result when input address less than truncation address.", 0, truncationAddress.compareTo(a));

        // Address in same ledger.
        a = m.getNextAddress(new LedgerAddress(firstLedgerSeq, firstLedgerId, 0), 2);
        Assert.assertEquals("Unexpected ledger id when result should be in the same ledger.", firstLedgerId, a.getLedgerId());
        Assert.assertEquals("Unexpected entry id when result should be in the same ledger.", 1, a.getEntryId());

        // Address in next ledger.
        for (int i = 0; i < m.getLedgers().size(); i++) {
            val lm = m.getLedgers().get(i);
            a = m.getNextAddress(new LedgerAddress(lm.getSequence(), lm.getLedgerId(), 3), 3);
            if (i == m.getLedgers().size() - 1) {
                Assert.assertNull("Unexpected result when reached the end of the log.", a);
            } else {
                val nextLm = m.getLedgers().get(i + 1);
                Assert.assertEquals("Unexpected ledger id when result should be in the next ledger.", nextLm.getLedgerId(), a.getLedgerId());
                Assert.assertEquals("Unexpected entry id when result should be in the next ledger.", 0, a.getEntryId());
            }
        }

        // Address in next ledger but current address is between ledgers (simulate some old address truncated out).
        a = m.getNextAddress(new LedgerAddress(firstLedgerSeq + 1, firstLedgerId + 1, 3), 3);
        Assert.assertEquals("Unexpected ledger id when result should be in the next ledger.", secondLedgerId, a.getLedgerId());
        Assert.assertEquals("Unexpected entry id when result should be in the next ledger.", 0, a.getEntryId());
    }

    /**
     * Tests serialization/deserialization.
     */
    @Test(timeout = 5000)
    public void testSerialization() throws Exception {
        Supplier<Long> nextLedgerId = new AtomicLong()::incrementAndGet;
        LogMetadata m1 = null;
        val lacs = new HashMap<Long, Long>();
        for (int i = 0; i < LEDGER_COUNT; i++) {
            long ledgerId = nextLedgerId.get() * 2;
            if (m1 == null) {
                m1 = new LogMetadata(ledgerId).withUpdateVersion(i);
            } else {
                m1 = m1.addLedger(ledgerId).withUpdateVersion(i);
            }

            if (i % 2 == 0) {
                // Every other Ledger, update the LastAddConfirmed.
                lacs.put((long) i, (long) i + 1);
            }
        }

        m1 = m1.updateLedgerStatus(lacs);
        val serialization = LogMetadata.SERIALIZER.serialize(m1);
        val m2 = LogMetadata.SERIALIZER.deserialize(serialization);

        Assert.assertEquals("Unexpected epoch.", m1.getEpoch(), m2.getEpoch());
        Assert.assertEquals("Unexpected TruncationAddress.", m1.getTruncationAddress().getSequence(), m2.getTruncationAddress().getSequence());
        Assert.assertEquals("Unexpected TruncationAddress.", m1.getTruncationAddress().getLedgerId(), m2.getTruncationAddress().getLedgerId());
        AssertExtensions.assertListEquals("Unexpected ledgers.", m1.getLedgers(), m2.getLedgers(),
                (l1, l2) -> l1.getSequence() == l2.getSequence() && l1.getLedgerId() == l2.getLedgerId() && l1.getStatus() == l2.getStatus());
    }

    /**
     * Tests {@link ReadOnlyBookkeeperLogMetadata#equals}.
     */
    @Test
    public void testEquals() {
        val m1 = LogMetadata.builder()
                .enabled(true)
                .epoch(1)
                .updateVersion(2)
                .truncationAddress(new LedgerAddress(100L, 100L))
                .ledgers(Arrays.asList(new LedgerMetadata(1000L, 100, LedgerMetadata.Status.Unknown),
                        new LedgerMetadata(2000L, 200, LedgerMetadata.Status.NotEmpty)))
                .build();

        // Check against null.
        Assert.assertFalse(m1.equals(null));

        // A copy of itself should be identical.
        Assert.assertTrue(m1.equals(copyOf(m1)));

        // Enabled/disabled.
        Assert.assertFalse(m1.equals(m1.asDisabled()));
        Assert.assertTrue(m1.equals(m1.asDisabled().asEnabled()));

        // Update version is not part of the equality check.
        Assert.assertTrue(m1.equals(m1.withUpdateVersion(234)));

        // Truncation address.
        Assert.assertFalse(m1.equals(m1.truncate(new LedgerAddress(200L, 100L))));

        // Ledgers.
        Assert.assertFalse(m1.equals(m1.updateLedgerStatus(Collections.singletonMap(1000L, Ledgers.NO_ENTRY_ID + 1))));
        Assert.assertFalse(m1.equals(m1.addLedger(300)));
    }

    private LogMetadata copyOf(LogMetadata m) {
        return LogMetadata.builder()
                .enabled(m.isEnabled())
                .epoch(m.getEpoch())
                .updateVersion(m.getUpdateVersion())
                .truncationAddress(new LedgerAddress(m.getTruncationAddress().getSequence(), m.getTruncationAddress().getLedgerSequence()))
                .ledgers(m.getLedgers().stream().map(lm -> new LedgerMetadata(lm.getLedgerId(), lm.getSequence(), lm.getStatus())).collect(Collectors.toList()))
                .build();
    }

    private void checkLedgerIds(List<Long> expectedLedgerIds, LogMetadata metadata) {
        val actualLedgerIds = metadata.getLedgers().stream().map(LedgerMetadata::getLedgerId).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected ledger ids.", expectedLedgerIds, actualLedgerIds, Long::equals);
    }
}
