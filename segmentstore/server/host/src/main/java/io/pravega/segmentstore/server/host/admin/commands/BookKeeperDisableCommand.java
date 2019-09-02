/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;

/**
 * Disables a BookKeeperLog.
 */
public class BookKeeperDisableCommand extends BookKeeperCommand {
    private static final int MAX_RETRIES = 10;
    private static final Retry.RetryAndThrowBase<? extends Exception> DISABLE_RETRY = Retry
            .withExpBackoff(100, 2, MAX_RETRIES, 1000)
            .retryWhen(ex -> ex instanceof DataLogWriterNotPrimaryException);

    /**
     * Creates a new instance of the BookKeeperDisableCommand.
     *
     * @param args The arguments for the command.
     */
    BookKeeperDisableCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int logId = getIntArg(0);

        @Cleanup
        val context = createContext();
        @Cleanup
        val log = context.logFactory.createDebugLogWrapper(logId);

        // Display a summary of the BookKeeperLog.
        val m = log.fetchMetadata();
        outputLogSummary(logId, m);
        if (m == null) {
            // Nothing else to do.
            return;
        } else if (!m.isEnabled()) {
            output("BookKeeperLog '%s' is already disabled.", logId);
            return;
        }

        output("BookKeeperLog '%s' is about to be DISABLED.", logId);
        output("\tIts SegmentContainer will shut down and it will not be able to restart until re-enabled.");
        output("\tNo request on this SegmentContainer can be processed until that time (OUTAGE ALERT).");
        if (!confirmContinue()) {
            output("Not disabling anything at this time.");
            return;
        }

        try {
            AtomicInteger count = new AtomicInteger(0);
            // We may be competing with a rather active Log which updates its metadata quite frequently, so try a few
            // times to acquire the ownership.
            DISABLE_RETRY.run(() -> {
                output("Acquiring ownership (attempt %d/%d) ...", count.incrementAndGet(), MAX_RETRIES);
                log.disable();
                output("BookKeeperLog '%s' has been disabled.", logId);
                return null;
            });
        } catch (Exception ex) {
            Throwable cause = ex;
            if (cause instanceof RetriesExhaustedException && cause.getCause() != null) {
                cause = cause.getCause();
            }
            output("Disable failed: %s.", cause.getMessage());
        }

        output("Current metadata:");
        val m2 = log.fetchMetadata();
        outputLogSummary(logId, m2);
    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "disable",
                "Disables a BookKeeperLog by open-fencing it and updating its metadata in ZooKeeper (with the Enabled flag set to 'false').",
                new ArgDescriptor("log-id", "Id of the log to disable."));
    }
}
