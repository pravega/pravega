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

/**
 * List all ledgers in the configured ledgers path for debug purposes.
 */
public class BookKeeperListAllLedgersCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the BookKeeperListAllLedgersCommand.
     *
     * @param args The arguments for the command.
     */
    public BookKeeperListAllLedgersCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(0);

        @Cleanup
        val context = createContext();
        ClientConfiguration config = new ClientConfiguration()
                .setMetadataServiceUri("zk://" + this.getServiceConfig().getZkURL() + context.bookKeeperConfig.getBkLedgerPath());
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
                    candidateLedgers.add(Ledgers.openRead(ledgerId, bkClient, context.bookKeeperConfig));
                }
            }

            // Output all the ledgers found.
            output("List of ledgers in the system: ");
            for (ReadHandle rh : candidateLedgers) {
                output("%s, length: %d, lastEntryConfirmed: %d, ledgerMetadata: %s, bookieLogID: %d",
                        rh.toString(), rh.getLength(), rh.readLastAddConfirmed(), rh.getLedgerMetadata().toSafeString(),
                        Ledgers.getBookKeeperLogId(rh));
            }
        } finally {
            // Closing opened ledgers.
            closeBookkeeperReadHandles(candidateLedgers);
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "list-ledgers",
                "List all the ledgers in Bookkeeper.");
    }
}
