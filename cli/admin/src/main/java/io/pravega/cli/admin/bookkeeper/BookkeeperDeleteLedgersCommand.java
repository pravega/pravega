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
import io.pravega.segmentstore.storage.impl.bookkeeper.Ledgers;
import lombok.Cleanup;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BookkeeperDeleteLedgersCommand  extends BookKeeperCommand {

    public BookkeeperDeleteLedgersCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);
        int logId = getIntArg(0);
        long startId = getLongArg(1);

        @Cleanup
        val context = createContext();
        @Cleanup
        val log = context.logFactory.createDebugLogWrapper(logId);

        // Display a summary of the BookKeeperLog.
        val m = log.fetchMetadata();
        if (m == null) {
            output("BookKeeperLog '%s' does not exist.", logId);
            return;
        }

        outputLogSummary(logId, m);

        output("Ledgers will be permanently deleted from bookkeeper log '%s' " +
                "starting with ledger id '%s' and reconcile will happen automatically ", logId, startId);
        if (!confirmContinue()) {
            output("Exiting delete-ledgers command");
            return;
        }
        try {
            log.deleteLedgersStartingWithId(startId);
            output("Deleted ledgers from bookkeeper log '%s' starting with ledger id '%s' ", logId, startId);
        } catch (Exception ex) {
            output("Delete ledgers failed: " + ex.getMessage());
            return;
        }
        output("Starting force over-write to recover log");
        // over-write metadata
        ClientConfiguration config = new ClientConfiguration()
                .setMetadataServiceUri( "zk://" + this.getServiceConfig().getZkURL() + context.bookKeeperConfig.getBkLedgerPath());
        @Cleanup
        BookKeeper bkClient = BookKeeper.forConfig(config).build();
        @Cleanup
        LedgerManager manager = bkClient.getLedgerManager();
        LedgerManager.LedgerRangeIterator ledgerRangeIterator = manager.getLedgerRanges(Long.MAX_VALUE);
        List<ReadHandle> candidateLedgers = new ArrayList<>();
        try {
            while (ledgerRangeIterator.hasNext()) {
                LedgerManager.LedgerRange lr = ledgerRangeIterator.next();
                for (long ledgerId : lr.getLedgers()) {
                    ReadHandle readHandle = Ledgers.openRead(ledgerId, bkClient, context.bookKeeperConfig);
                    if (Ledgers.getBookKeeperLogId(readHandle) == logId) {
                        candidateLedgers.add(readHandle);
                    }
                }
            }
            // If there are no candidate ledgers then the last or last 2 ledgers are missing.
            // So checking if log also has no ledger information and is empty.
            if (candidateLedgers.isEmpty() && m.getLedgers().isEmpty()) {
                output("No candidate ledgers to over-write.");
                return;
            }
            // Confirm with user prior executing the command.
            output("Candidate ledgers for over-write: %s",
                    candidateLedgers.stream().map(String::valueOf).collect(Collectors.joining(",")));
            output("BookKeeperLog '%s' over-write is about to be executed.", logId);

            log.reconcileLedgers(candidateLedgers, true);
        } finally {
            // Closing opened ledgers.
            closeBookkeeperReadHandles(candidateLedgers);
        }
        val updatedLog = log.fetchMetadata();
        outputLogSummary(logId, updatedLog);
        output("Delete ledgers command completed");
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "delete-ledgers",
                "Deletes ledgers starting with given ledger-id.",
                new ArgDescriptor("log-id", "Id of the log to delete ledgers from."),
                new ArgDescriptor("ledger-id", "Id of the starting ledger to be deleted."));
    }
}
