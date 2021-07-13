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
 * Enables a previously disabled BookKeeperLog.
 */
public class BookKeeperEnableCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the BookKeeperEnableCommand class.
     * @param args The arguments for the command.
     */
    public BookKeeperEnableCommand(CommandArgs args) {
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
        } else if (m.isEnabled()) {
            output("BookKeeperLog '%s' is already enabled.", logId);
            return;
        }

        output("BookKeeperLog '%s' is about to be ENABLED.", logId);
        if (!confirmContinue()) {
            output("Not enabling anything at this time.");
            return;
        }

        try {
            log.enable();
            output("BookKeeperLog '%s' has been enabled. It may take a few minutes for its SegmentContainer to resume operations.", logId);
        } catch (Exception ex) {
            output("Enable failed: " + ex.getMessage());
        }

        output("Current metadata:");
        val m2 = log.fetchMetadata();
        outputLogSummary(logId, m2);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "enable",
                "Enables a BookKeeperLog by updating its metadata in ZooKeeper (with the Enabled flag set to 'true').",
                new ArgDescriptor("log-id", "Id of the log to enable."));
    }
}
