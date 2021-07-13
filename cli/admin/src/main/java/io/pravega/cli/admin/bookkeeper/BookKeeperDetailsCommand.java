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
import io.pravega.segmentstore.storage.impl.bookkeeper.LedgerMetadata;
import lombok.Cleanup;
import lombok.Data;
import lombok.val;
import org.apache.bookkeeper.client.LedgerHandle;
import java.util.stream.Collectors;

/**
 * Fetches details about a BookKeeperLog.
 */
public class BookKeeperDetailsCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the BookKeeperDetailsCommand.
     *
     * @param args The arguments for the command.
     */
    public BookKeeperDetailsCommand(CommandArgs args) {
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
                lh = (LedgerHandle) log.openLedgerNoFencing(lm);
                val bkLm = context.bkAdmin.getLedgerMetadata(lh);
                prettyJSONOutput("ledger_details", new LedgerDetails(lm.getLedgerId(), lm.getSequence(), String.valueOf(lm.getStatus()),
                        lh.getLastAddConfirmed(), lh.getLength(), lh.getNumBookies(), lh.getNumFragments(),
                        bkLm.getEnsembleSize(), bkLm.getWriteQuorumSize(), bkLm.getAckQuorumSize(), getEnsembleDescription(bkLm)));
            } catch (Exception ex) {
                System.err.println("Exception executing BK details command: " + ex.getMessage());
                output("\tLedger %d: Seq = %d, Status = %s. BK: %s",
                        lm.getLedgerId(), lm.getSequence(), lm.getStatus(), ex.getMessage());
            } finally {
                if (lh != null) {
                    lh.close();
                }
            }
        }
    }

    private String getEnsembleDescription(org.apache.bookkeeper.client.api.LedgerMetadata bkLm) {
        return bkLm.getAllEnsembles().entrySet().stream()
                   .map(e -> String.format("%d: [%s]", e.getKey(), e.getValue().stream().map(Object::toString).collect(Collectors.joining(","))))
                   .collect(Collectors.joining(","));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "details",
                "Lists metadata details about a BookKeeperLog, including BK Ledger information.",
                new ArgDescriptor("log-id", "Id of the log to get details for."));
    }

    @Data
    private static class LedgerDetails {
        private final long ledger;
        private final int seq;
        private final String status;
        private final long lac;
        private final long length;
        private final long numBookies;
        private final long numFragments;
        private final int ensembleSize;
        private final int writeQuorumSize;
        private final int ackQuorumSize;
        private final String ensembles;
    }
}
