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
package io.pravega.cli.admin.bookkeeper;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.impl.bookkeeper.DebugBookKeeperLogWrapper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;

/**
 * Identifies and deletes orphaned BookKeeper ledgers.
 */
public class BookKeeperCleanupCommand extends BookKeeperCommand {
    /**
     * Creates a new instance of the BookKeeperCleanupCommand.
     *
     * @param args The arguments for the command.
     */
    public BookKeeperCleanupCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(0);

        @Cleanup
        val context = createContext();

        // Get all BK ledger ids.
        output("Searching for all the ledgers ...");
        @Cleanup
        val bkAdmin = new BookKeeperAdmin((BookKeeper) context.logFactory.getBookKeeperClient());
        val allLedgerIds = new ArrayList<Long>();
        bkAdmin.listLedgers().forEach(allLedgerIds::add);

        output("Searching for all referenced ledgers ...");

        // We will not be deleting any ledger id above the highest referenced Ledger Id since we may be inadvertently
        // deleting freshly created Ledgers that have not yet been added to a Ledger Metadata yet (BookKeeperLog.initialize
        // first creates a new Ledger and then updates the Metadata in ZK with its existence).
        AtomicLong highestReferencedLedgerId = new AtomicLong();
        val referencedLedgerIds = new HashSet<Long>();
        collectAllReferencedLedgerIds(referencedLedgerIds, context);
        highestReferencedLedgerId.set(referencedLedgerIds.stream().max(Long::compareTo).orElse(-1L));

        // We want to do our due diligence and verify there are no more other BookKeeperLogs that the user hasn't told us about.
        output("Searching for possible other BookKeeperLogs ...");
        checkForExtraLogs(context);

        // Determine deletion candidates.
        val deletionCandidates = allLedgerIds.stream()
                                             .filter(id -> id < highestReferencedLedgerId.get() && !referencedLedgerIds.contains(id))
                                             .collect(Collectors.toList());
        output("\nTotal Count: %d, Referenced Count: %d, Highest Referenced Id: %s, To Delete Count: %d.",
                allLedgerIds.size(), referencedLedgerIds.size(), highestReferencedLedgerId, deletionCandidates.size());
        if (deletionCandidates.isEmpty()) {
            output("There are no Ledgers eligible for deletion at this time.");
            return;
        }

        output("\nDeletion candidates:");
        listCandidates(deletionCandidates, context);
        if (!confirmContinue()) {
            output("Not deleting anything at this time.");
            return;
        }

        // Search again for referenced ledger ids, in case any new ones were just referenced.
        collectAllReferencedLedgerIds(referencedLedgerIds, context);
        highestReferencedLedgerId.set(referencedLedgerIds.stream().max(Long::compareTo).orElse(-1L));
        deleteCandidates(deletionCandidates, referencedLedgerIds, context);
    }

    @VisibleForTesting
    void deleteCandidates(List<Long> deletionCandidates, Collection<Long> referencedLedgerIds, Context context) {
        for (long ledgerId : deletionCandidates) {
            if (referencedLedgerIds.contains(ledgerId)) {
                output("Not deleting Ledger %d because is is now referenced.", ledgerId);
                continue;
            }

            try {
                Exceptions.handleInterrupted(() -> context.logFactory.getBookKeeperClient().newDeleteLedgerOp().withLedgerId(ledgerId));
                output("Deleted Ledger %d.", ledgerId);
            } catch (Exception ex) {
                output("FAILED to delete Ledger %d: %s.", ledgerId, ex.getMessage());
            }
        }
    }

    @VisibleForTesting
    void listCandidates(List<Long> deletionCandidates, Context context) {
        for (long ledgerId : deletionCandidates) {
            try {
                val lh = context.bkAdmin.openLedgerNoRecovery(ledgerId);
                output("\tLedger %d: LAC=%d, Length=%d, Bookies=%d, Frags=%d.",
                        ledgerId, lh.getLastAddConfirmed(), lh.getLength(), lh.getNumBookies(), lh.getNumFragments());
            } catch (Exception ex) {
                output("Ledger %d: %s.", ledgerId, ex.getMessage());
            }
        }
    }

    private void collectAllReferencedLedgerIds(Collection<Long> referencedLedgerIds, Context context) throws Exception {
        referencedLedgerIds.clear();
        for (int logId = 0; logId < context.serviceConfig.getContainerCount(); logId++) {
            @Cleanup
            DebugBookKeeperLogWrapper log = context.logFactory.createDebugLogWrapper(logId);
            val m = log.fetchMetadata();
            if (m == null) {
                continue;
            }

            for (val lm : m.getLedgers()) {
                referencedLedgerIds.add(lm.getLedgerId());
            }
        }
    }

    private void checkForExtraLogs(Context context) throws Exception {
        val maxLogId = context.serviceConfig.getContainerCount() * 10;
        for (int logId = context.serviceConfig.getContainerCount(); logId < maxLogId; logId++) {
            @Cleanup
            DebugBookKeeperLogWrapper log = context.logFactory.createDebugLogWrapper(logId);
            val m = log.fetchMetadata();
            if (m != null) {
                throw new Exception(String.format("Discovered BookKeeperLog %d which is beyond the maximum log id (%d) as specified in the configuration.",
                        logId, context.serviceConfig.getContainerCount() - 1));
            }
        }
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(BookKeeperCommand.COMPONENT,
                "cleanup",
                "Removes orphan BookKeeper Ledgers that are not used by any BookKeeperLog.");
    }
}
