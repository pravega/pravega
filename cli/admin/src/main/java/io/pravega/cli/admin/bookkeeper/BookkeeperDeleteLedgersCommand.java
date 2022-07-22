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
        outputLogSummary(logId, m);
        if (m == null || m.isEnabled()) {
            String message = (m == null) ? "BookKeeperLog '%s' does not exist." :
                    "BookKeeperLog '%s' is enabled. Please, disable it before executing this command.";
            output(message, logId);
            return;
        }
        output("Ledgers will be permanently deleted from bookkeeper log '%s' starting with ledger id '%s' ", logId, startId);
        if (!confirmContinue()) {
            output("Not reconciling anything at this time.");
            return;
        }
        try {
            log.deleteLedgersStartingWithId(startId);
            output("Deleted ledgers from bookkeeper log '%s' starting with ledger id '%s' ", logId, startId);
        } catch (Exception ex) {
            output("Delete ledgers failed: " + ex.getMessage());
        }
        output("Delete ledgers command successful");
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "delete-ledgers",
                "Deletes ledgers starting with given ledger-id.",
                new ArgDescriptor("log-id", "Id of the log to delete ledgers from."),
                new ArgDescriptor("ledger-id", "Id of the starting ledger to be deleted."));
    }
}
