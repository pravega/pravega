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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.LedgerMetadata;
import io.pravega.segmentstore.storage.impl.bookkeeper.Ledgers;
import lombok.Cleanup;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.meta.LedgerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BookKeeperLogReconciliationCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the BookKeeperLogReconciliationCommand.
     *
     * @param args The arguments for the command.
     */
    public BookKeeperLogReconciliationCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int logId = getIntArg(0);

        // Ensure that the Bookkeeper log is disabled; abort otherwise.
        @Cleanup
        val context = createContext();
        @Cleanup
        val log = context.logFactory.createDebugLogWrapper(logId);
        // Display a summary of the BookKeeperLog.
        val m = log.fetchMetadata();
        outputLogSummary(logId, m);
        if (m.isEnabled()) {
            output("BookKeeperLog '%s' is enabled. Please, disable it before executing this command.", logId);
            return;
        }

        // Once the Bookkeeper log is disabled, list all ledgers from this log. This implies to query all the ledgers
        // in Bookkeeper and filter out the ones related to BookkeeperLog id passed by parameter.
        @Cleanup
        BookKeeper bkClient = new BookKeeper(this.getServiceConfig().getZkURL());
        @Cleanup
        LedgerManager manager = bkClient.getLedgerManager();
        LedgerManager.LedgerRangeIterator ledgerRangeIterator = manager.getLedgerRanges(Long.MAX_VALUE);
        List<ReadHandle> candidateLedgers = new ArrayList<>();
        while (ledgerRangeIterator.hasNext()) {
            LedgerManager.LedgerRange lr = ledgerRangeIterator.next();
            for (long ledgerId: lr.getLedgers()) {
                ReadHandle readHandle = Ledgers.openRead(ledgerId, bkClient, BookKeeperConfig.builder().build());
                if (Ledgers.getBookKeeperLogId(readHandle) == logId) {
                    candidateLedgers.add(readHandle);
                }
            }
        }

        // Confirm with user prior executing the command.
        output("Candidate ledgers for reconciliation: " +
                candidateLedgers.stream().map(String::valueOf).collect(Collectors.joining(",")));
        output("BookKeeperLog '%s' reconciliation is about to be executed.", logId);
        if (!confirmContinue()) {
            output("Not reconciling anything at this time.");
            return;
        }

        // Executing BookkeeperLog reconciliation.
        output("BookKeeperLog '%s': starting ledger reconciliation.", logId);
        log.reconcileLedgers(candidateLedgers);
        output("BookKeeperLog '%s': ledger reconciliation completed.", logId);

        // Closing opened ledgers.
        for (ReadHandle readHandle: candidateLedgers) {
            if (!readHandle.isClosed()) {
                readHandle.close();
            }
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "reconcile",
                "Allows reconstructing a BookkeeperLog metadata (stored in ZK) in case it got wiped out.",
                new ArgDescriptor("log-id", "Id of the log to reconcile/reconstruct."));
    }
}
