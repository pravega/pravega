/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.cli.admin.controller.metadata;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.ZKHelper;

public class ControllerMetadataViewReaderInfoCommand extends ControllerMetadataCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataViewReaderInfoCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(3);

        final String hostId = getArg(0);
        final String readerGroupName = getArg(1);
        final String readerId = getArg(2);

        try {
            ZKHelper zkHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            output("reader-metadata: \n %s", zkHelper.getMetaDataForReader(getReaderPath(hostId, readerGroupName, readerId)));
        } catch (Exception e) {
            output("Exception accessing to reader metadata : " + e.getMessage());
        }

    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-reader", "Get the reader metadata of reader belonging to internal reader group for a particular controller host",
                new ArgDescriptor("host-id", "Id of controller host"),
                new ArgDescriptor("reader-group-name", "Name of the ReaderGroup"),
                new ArgDescriptor("reader-id", "Id of reader to view reader information"));
    }


}
