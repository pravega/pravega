/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import io.pravega.segmentstore.storage.impl.bookkeeper.LedgerMetadata;
import lombok.Cleanup;
import lombok.val;
import org.apache.bookkeeper.client.LedgerHandle;

/**
 * Fetches details about a BookKeeperLog.
 */
public class BookKeeperDetailsCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the BookKeeperDetailsCommand.
     *
     * @param args The arguments for the command.
     */
    BookKeeperDetailsCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        int logId = getIntArg(0);

        @Cleanup
        val context = getContext();
        @Cleanup
        val log = context.logFactory.createDebugLogWrapper(logId);
        val m = log.fetchMetadata();
        outputLogSummary(logId, m);
        if (m == null) {
            // Nothing else to do.
            return;
        }

        if (m.getLedgers().size() == 0) {
            output("There are no ledgers for Log %s.", logId);
            return;
        }

        for (LedgerMetadata lm : m.getLedgers()) {
            LedgerHandle lh = null;
            try {
                lh = log.openLedgerNoFencing(lm);
                output("\tLedger %d: Seq=%d, Status=%s, BK.LAC=%d, BK.Length=%d, BK.Bookies=%d, BK.Frags=%d.",
                        lm.getLedgerId(), lm.getSequence(), lm.getStatus(),
                        lh.getLastAddConfirmed(), lh.getLength(), lh.getNumBookies(), lh.getNumFragments());
            } catch (Exception ex) {
                output("\tLedger %d: Seq = %d, Status = %s. BK: %s",
                        lm.getLedgerId(), lm.getSequence(), lm.getStatus(), ex.getMessage());
            } finally {
                if (lh != null) {
                    lh.close();
                }
            }
        }
    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "details",
                "Lists metadata details about a BookKeeperLog, as well as optional Ledger information.",
                new ArgDescriptor("log-id", "Id of the log to get details for."),
                new ArgDescriptor("from-bookkeeper", "Whether to include ledger info from BK as well (true|false)."));
    }
}
