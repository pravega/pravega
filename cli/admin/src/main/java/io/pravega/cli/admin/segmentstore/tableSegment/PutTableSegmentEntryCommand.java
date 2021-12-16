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
package io.pravega.cli.admin.segmentstore.tableSegment;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

public class PutTableSegmentEntryCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of the PutTableSegmentEntryCommand.
     *
     * @param args The arguments for the command.
     */
    public PutTableSegmentEntryCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(4);
        ensureSerializersExist();

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String segmentStoreHost = getArg(1);
        final String key = getArg(2);
        final String value = getArg(3);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        long version = updateTableEntry(fullyQualifiedTableSegmentName, key, value, segmentStoreHost, adminSegmentHelper);
        output("Successfully updated the key %s in table %s with version %s", key, fullyQualifiedTableSegmentName, version);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "put", "Update the given key in the table with the provided value." +
                "Use the command \"table-segment set-serializer <serializer-name>\" to use the appropriate serializer before using this command.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get info from."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."),
                new ArgDescriptor("key", "The key whose value is to be updated."),
                new ArgDescriptor("value", "The new value, provided as \"key1=value1;key2=value2;key3=value3;...\"."));
    }
}
