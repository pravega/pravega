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
package io.pravega.cli.admin.controller.metadata;

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.json.ControllerMetadataJsonSerializer;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer;
import io.pravega.client.tables.impl.TableSegmentEntry;
import lombok.Cleanup;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;

import java.io.FileWriter;

import static io.pravega.cli.admin.utils.FileHelper.createFileAndDirectory;

public class ControllerMetadataGetEntryCommand extends ControllerMetadataCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataGetEntryCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        Preconditions.checkArgument(getArgCount() >= 3 && getArgCount() < 5, "Incorrect argument count.");

        final String tableName = getArg(0);
        final String key = getArg(1);
        final String segmentStoreHost = getArg(2);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(tableName, key);
        TableSegmentEntry entry = getTableEntry(tableName, key, segmentStoreHost, adminSegmentHelper);
        if (entry == null) {
            return;
        }
        val value = serializer.deserialize(getByteBuffer(entry.getValue()));
        output("For the given key: %s", key);
        if (getArgCount() == 4) {
            final String jsonFile = getArg(3);

            ControllerMetadataJsonSerializer jsonSerializer = new ControllerMetadataJsonSerializer();
            @Cleanup
            FileWriter writer = new FileWriter(createFileAndDirectory(jsonFile));
            writer.write(jsonSerializer.toJson(value));
            writer.flush();

            output("Successfully wrote the value to %s in JSON.", jsonFile);
        } else {
            userFriendlyOutput(value.toString(), serializer.getMetadataType());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get", "Get the value for the specified key from the specified controller metadata table.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get the entry from. " +
                        "Run \"controller-metadata tables-info\" to get information about the controller metadata tables."),
                new ArgDescriptor("key", "The key to be queried."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."),
                new ArgDescriptor("json-file", "An optional argument which, if provided, will write the value as " +
                        "JSON into the given file path.", true));
    }
}
