package io.pravega.cli.admin.bookkeeper;

import io.pravega.cli.admin.CommandArgs;
import lombok.Cleanup;
import lombok.val;

public class BookkeeperDeleteLedgersCommand  extends BookKeeperCommand{

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
        if (m == null) {
            output("BookkeeperLog '%s' does not exists");
            return;
        }
        if (m.isEnabled()) {
            output("BookKeeperLog '%s' is enabled, disable the log before trying delete ledgers command", logId);
            return;
        }
        try {
            log.deleteLedgersStartingWithId(startId);
            output("Deleted ledgers from bookkeeper log '%s' starting with ledger id '%s' ", logId, startId);
        } catch (Exception ex) {
            output("Delete ledgers failed: " + ex.getMessage());
        }

        output("Current metadata:");
        val m2 = log.fetchMetadata();
        outputLogSummary(logId, m2);

    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "delete-ledgers",
                "Deletes ledgers starting with given ledger-id.",
                new ArgDescriptor("log-id", "Id of the log to delete ledgers from."),
                new ArgDescriptor("ledger-id", "Id of the starting ledger to be deleted."));
    }
}
