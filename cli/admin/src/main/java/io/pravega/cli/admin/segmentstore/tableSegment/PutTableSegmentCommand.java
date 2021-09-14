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

import com.google.common.base.Charsets;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class PutTableSegmentCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of the PutTableSegmentCommand.
     *
     * @param args The arguments for the command.
     */
    public PutTableSegmentCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(4);

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String key = getArg(1);
        final String value = getArg(2);
        final String segmentStoreHost = getArg(3);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        TableSegmentEntry updatedEntry = TableSegmentEntry.unversioned(key.getBytes(Charsets.UTF_8),
                getCommandArgs().getState().getValueSerializer().serialize(value).array());
        CompletableFuture<List<TableSegmentKeyVersion>> reply = adminSegmentHelper.updateTableEntries(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(updatedEntry), super.authHelper.retrieveMasterToken(), 0L);
        // TODO: Serialization logic here.
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "put", "Update the key within table segment with the provided value.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get info from."),
                new ArgDescriptor("key", "The key for which the value is to be updated."),
                new ArgDescriptor("value", "The new value to be updated."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
