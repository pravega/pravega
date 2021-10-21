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
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the LedgerAddress class.
 */
public class LedgerAddressTests {
    private static final int LEDGER_COUNT = 10;
    private static final int ENTRY_COUNT = 10;

    /**
     * Tests the various properties as well as translation from the encoded sequence and back.
     */
    @Test(timeout = 5000)
    public void testSequence() {
        for (int ledgerSeq = 0; ledgerSeq < LEDGER_COUNT; ledgerSeq++) {
            long ledgerId = (ledgerSeq + 1) * 31;
            for (long entryId = 0; entryId < ENTRY_COUNT; entryId++) {
                val a = new LedgerAddress(ledgerSeq, ledgerId, entryId);
                assertEquals(a, ledgerSeq, ledgerId, entryId);
                val a2 = new LedgerAddress(a.getSequence(), ledgerId);
                assertEquals(a2, ledgerSeq, ledgerId, entryId);
            }
        }
    }

    /**
     * Tests the Compare method.
     */
    @Test(timeout = 5000)
    public void testCompare() {
        val addresses = new ArrayList<LedgerAddress>();
        for (int ledgerSeq = 0; ledgerSeq < LEDGER_COUNT; ledgerSeq++) {
            long ledgerId = (ledgerSeq + 1) * 31;
            for (long entryId = 0; entryId < ENTRY_COUNT; entryId++) {
                addresses.add(new LedgerAddress(ledgerSeq, ledgerId, entryId));
            }
        }

        for (int i = 0; i < addresses.size() / 2; i++) {
            val a1 = addresses.get(i);
            val a2 = addresses.get(addresses.size() - i - 1);
            val result1 = a1.compareTo(a2);
            val result2 = a2.compareTo(a1);
            AssertExtensions.assertLessThan("Unexpected when comparing smaller to larger.", 0, result1);
            AssertExtensions.assertGreaterThan("Unexpected when comparing larger to smaller.", 0, result2);
            Assert.assertEquals("Unexpected when comparing to itself.", 0, a1.compareTo(a1));
        }
    }

    private void assertEquals(LedgerAddress a, int ledgerSeq, long ledgerId, long entryId) {
        Assert.assertEquals("Unexpected ledger sequence.", ledgerSeq, a.getLedgerSequence());
        Assert.assertEquals("Unexpected ledger id.", ledgerId, a.getLedgerId());
        Assert.assertEquals("Unexpected entry id.", entryId, a.getEntryId());
    }
}
