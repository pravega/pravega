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
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.ReadOnlyBookkeeperLogMetadata;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.curator.framework.CuratorFramework;

import java.util.List;

/**
 * Base for any BookKeeper-related commands.
 */
abstract class BookKeeperCommand extends AdminCommand {
    static final String COMPONENT = "bk";

    BookKeeperCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Outputs a summary for the given Log.
     *
     * @param logId The Log Id.
     * @param m     The Log Metadata for the given Log Id.
     */
    void outputLogSummary(int logId, ReadOnlyBookkeeperLogMetadata m) {
        if (m == null) {
            prettyJSONOutput("log_no_metadata)", logId);
        } else {
            prettyJSONOutput("log_summary", new LogSummary(logId, m.getEpoch(), m.getUpdateVersion(), m.isEnabled(),
                    m.getLedgers().size(), String.valueOf(m.getTruncationAddress())));
        }
    }

    /**
     * Creates a new Context to be used by the BookKeeper command.
     *
     * @return A new Context.
     * @throws DurableDataLogException If the BookKeeperLogFactory could not be initialized.
     */
    @VisibleForTesting
    public Context createContext() throws DurableDataLogException {
        val serviceConfig = getServiceConfig();
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                                       .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, serviceConfig.getZkURL()))
                                       .build().getConfig(BookKeeperConfig::builder);
        val zkClient = createZKClient();
        val factory = new BookKeeperLogFactory(bkConfig, zkClient, getCommandArgs().getState().getExecutor());
        try {
            factory.initialize();
        } catch (DurableDataLogException ex) {
            zkClient.close();
            throw ex;
        }

        val bkAdmin = new BookKeeperAdmin((BookKeeper) factory.getBookKeeperClient());
        return new Context(serviceConfig, bkConfig, zkClient, factory, bkAdmin);
    }

    @RequiredArgsConstructor(access = AccessLevel.PROTECTED)
    protected static class Context implements AutoCloseable {
        final ServiceConfig serviceConfig;
        final BookKeeperConfig bookKeeperConfig;
        final CuratorFramework zkClient;
        final BookKeeperLogFactory logFactory;
        final BookKeeperAdmin bkAdmin;

        @Override
        @SneakyThrows(BKException.class)
        public void close() {
            this.logFactory.close();
            this.zkClient.close();

            // There is no need to close the BK Admin object since it doesn't own anything; however it does have a close()
            // method and it's a good idea to invoke it.
            Exceptions.handleInterrupted(this.bkAdmin::close);
        }
    }

    protected void closeBookkeeperReadHandles(List<ReadHandle> ledgers) {
        for (ReadHandle readHandle: ledgers) {
            try {
                readHandle.close();
            } catch (Exception e) {
                output("Error while attempting to close ledger %d: %s", readHandle.getId(), e.getMessage());
            }
        }
    }

    @Data
    @AllArgsConstructor
    private static class LogSummary {
        private int logId;
        private long epoch;
        private int version;
        private boolean enabled;
        private int ledgers;
        private String truncation;
    }
}
