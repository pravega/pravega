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
import io.netty.buffer.ByteBuf;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import lombok.Cleanup;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GetTableSegmentCommand extends TableSegmentCommand {

    /**
     * Creates a new instance of the GetTableSegmentCommand.
     *
     * @param args The arguments for the command.
     */
    public GetTableSegmentCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(3);

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String key = getArg(1);
        final String segmentStoreHost = getArg(2);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<List<TableSegmentEntry>> reply = adminSegmentHelper.readTable(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(TableSegmentKey.unversioned(key.getBytes())), super.authHelper.retrieveMasterToken(), 0L);

        val data = getCommandArgs().getState().getValueSerializer().deserialize(ByteBuffer.wrap(reply.join().get(0).getValue().array()));
        output("The value: %s", data);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get", "Get the value of the provided key from the table segment",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table segment to get info from."),
                new ArgDescriptor("key", "The key for which the value is being queried."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
