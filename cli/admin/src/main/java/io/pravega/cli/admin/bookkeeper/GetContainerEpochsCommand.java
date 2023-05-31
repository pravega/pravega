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

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.DebugBookKeeperLogWrapper;
import lombok.Cleanup;
import lombok.val;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static io.pravega.cli.admin.utils.FileHelper.createFileAndDirectory;

public class GetContainerEpochsCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the GetContainerEpochsCommand.
     *
     * @param args The arguments for the command.
     */
    public static final String COLON_SEPARATOR = ":";
    private static final String NEW_LINE_SEPARATOR = "\n";
    public GetContainerEpochsCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws DurableDataLogException, IOException {
        ensureArgCount(1);

        final String fileName = getArg(0);
        @Cleanup
        val context = createContext();
        FileHelpers.deleteFileOrDirectory(new File(fileName));
        @Cleanup
        FileWriter writer = new FileWriter(createFileAndDirectory(fileName));
        for (int containerId = 0; containerId < getServiceConfig().getContainerCount(); containerId++) {
            @Cleanup
            DebugBookKeeperLogWrapper log = context.logFactory.createDebugLogWrapper(containerId);
            val m = log.fetchMetadata();
            if (m != null) {
                writer.write(containerId + COLON_SEPARATOR + m.getEpoch() + NEW_LINE_SEPARATOR);
            }
        }
        writer.flush();
        writer.close();
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "get-container-epochs",
                "Gets and saves container epochs to the given filename");
    }
}
