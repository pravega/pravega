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

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.io.FileReader;
import java.nio.ByteBuffer;

public class ControllerMetadataUpdateEntryCommand extends ControllerMetadataCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataUpdateEntryCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(4);

        final String tableName = getArg(0);
        final String key = getArg(1);
        final String newValueFile = getArg(3);
        final String segmentStoreHost = getArg(4);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        ControllerMetadataSerializer serializer = new ControllerMetadataSerializer(tableName, key);

        @Cleanup
        FileReader reader = new FileReader(newValueFile);
        JsonReader jsonReader = new JsonReader(reader);
        jsonReader.setLenient(true);
        ByteBuffer updatedValue = serializer.serialize(new Gson().fromJson(jsonReader, serializer.getMetadataClass()));

        long version = updateTableEntry(tableName, key, updatedValue, segmentStoreHost, serializer, adminSegmentHelper);
        output("Successfully updated the key %s in table %s with version %s", key, tableName, version);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "update", "Update the given key in the table with the provided value.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to update. " +
                        "Run \"controller-metadata tables-info\" to get information about the controller metadata tables."),
                new ArgDescriptor("key", "The key to be updated."),
                new ArgDescriptor("new-value-file", "The path to the file containing the new value in JSON format."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
