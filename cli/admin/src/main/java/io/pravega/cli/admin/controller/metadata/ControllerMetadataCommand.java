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

import io.netty.buffer.ByteBuf;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.controller.ControllerCommand;
import io.pravega.cli.admin.serializers.controller.ControllerKeySerializer;
import io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static io.pravega.cli.admin.serializers.AbstractSerializer.parseStringData;

public abstract class ControllerMetadataCommand extends ControllerCommand {
    static final String COMPONENT = "controller-metadata";
    static final ControllerKeySerializer KEY_SERIALIZER = new ControllerKeySerializer();

    protected final GrpcAuthHelper authHelper;

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataCommand(CommandArgs args) {
        super(args);

        authHelper = new GrpcAuthHelper(true,
                "secret",
                600);
    }

    /**
     * Method to get the entry corresponding to the provided key in the table.
     *
     * @param tableName          The name of the table.
     * @param key                The key.
     * @param segmentStoreHost   The address of the segment store instance.
     * @param serializer         The valid {@link ControllerMetadataSerializer}.
     * @param adminSegmentHelper An instance of {@link AdminSegmentHelper}.
     * @return A string, obtained through deserialization, containing the contents of the queried table segment entry.
     */
    String getTableEntry(String tableName, String key, String segmentStoreHost,
                         ControllerMetadataSerializer serializer, AdminSegmentHelper adminSegmentHelper) {
        ByteArraySegment serializedKey = new ByteArraySegment(KEY_SERIALIZER.serialize(key));

        CompletableFuture<List<TableSegmentEntry>> reply = adminSegmentHelper.readTable(tableName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(TableSegmentKey.unversioned(serializedKey.getCopy())),
                authHelper.retrieveMasterToken(), 0L);
        return serializer.deserialize(getByteBuffer(reply.join().get(0).getValue()));
    }

    /**
     * Print the provided data in a user-friendly manner.
     *
     * @param data The data to be printed.
     * @param name A name describing the data.
     */
    void userFriendlyOutput(String data, String name) {
        Map<String, String> dataMap = parseStringData(data);
        // Case of primitive value eg: int, long, String, etc.
        if (dataMap.containsKey(name)) {
            output(dataMap.get(name));
            return;
        }
        output("%s metadata info: ", name);
        dataMap.forEach((k, v) -> output("%s = %s;", k, v));
    }

    /**
     * Method to convert a {@link ByteBuf} to a {@link ByteBuffer}.
     *
     * @param byteBuf The {@link ByteBuf} instance.
     * @return A {@link ByteBuffer} containing the data present in the provided {@link ByteBuf}.
     */
    ByteBuffer getByteBuffer(ByteBuf byteBuf) {
        final byte[] bytes = new byte[byteBuf.readableBytes()];
        final int readerIndex = byteBuf.readerIndex();
        byteBuf.getBytes(readerIndex, bytes);
        return ByteBuffer.wrap(bytes);
    }
}
