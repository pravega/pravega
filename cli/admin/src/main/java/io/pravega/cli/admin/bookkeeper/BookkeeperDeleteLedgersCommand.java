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
import lombok.Cleanup;
import lombok.val;

/**
 * This command delete all the ledgers of the log starting with and including given ledger-id.
 * It can be used as last resort to recover the log in case recovery fails with missing ledger(s).
 */
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

        log.markAsdisabled();

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
            output("Delete ledgers failed: Exiting " + ex.getMessage());
            return;
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
