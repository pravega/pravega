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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.segmentstore.SegmentStoreCommand;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.shared.protocol.netty.PravegaNodeUri;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class TableSegmentCommand extends SegmentStoreCommand {
    static final String COMPONENT = "table-segment";

    public TableSegmentCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Method to check if the serializers are set.
     */
    void ensureSerializersExist() {
        Preconditions.checkArgument(getCommandArgs().getState().getKeySerializer() != null && getCommandArgs().getState().getValueSerializer() != null,
                "The serializers have not been set. Use the command \"table-segment set-serializer <serializer-name>\" and try again.");
    }

    /**
     * Method to get the entry corresponding to the provided key in the table segment.
     *
     * @param tableSegmentName   The name of the table segment.
     * @param key                The key.
     * @param segmentStoreHost   The address of the segment store instance.
     * @param adminSegmentHelper An instance of {@link AdminSegmentHelper}.
     * @return A string, obtained through deserialization, containing the contents of the queried table segment entry.
     */
    String getTableEntry( String tableSegmentName,
                          String key,
                          String segmentStoreHost,
                          AdminSegmentHelper adminSegmentHelper) {
        ByteArraySegment serializedKey = new ByteArraySegment(getCommandArgs().getState().getKeySerializer().serialize(key));

        CompletableFuture<List<TableSegmentEntry>> reply = adminSegmentHelper.readTable(tableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(TableSegmentKey.unversioned(serializedKey.getCopy())),
                super.authHelper.retrieveMasterToken(), 0L);

        ByteBuffer serializedValue = getByteBuffer(reply.join().get(0).getValue());
        return getCommandArgs().getState().getValueSerializer().deserialize(serializedValue);
    }

    /**
     * Method to update the entry corresponding to the provided key in the table segment.
     *
     * @param tableSegmentName   The name of the table segment.
     * @param key                The key.
     * @param value              The entry to be updated in the table segment.
     * @param segmentStoreHost   The address of the segment store instance.
     * @param adminSegmentHelper An instance of {@link AdminSegmentHelper}
     * @return A long indicating the version obtained from updating the provided key in the table segment.
     */
    long updateTableEntry(String tableSegmentName,
                          String key, String value,
                          String segmentStoreHost,
                          AdminSegmentHelper adminSegmentHelper) {
        ByteArraySegment serializedKey = new ByteArraySegment(getCommandArgs().getState().getKeySerializer().serialize(key));
        ByteArraySegment serializedValue = new ByteArraySegment(getCommandArgs().getState().getValueSerializer().serialize(value));
        TableSegmentEntry updatedEntry = TableSegmentEntry.unversioned(serializedKey.getCopy(), serializedValue.getCopy());

        CompletableFuture<List<TableSegmentKeyVersion>> reply = adminSegmentHelper.updateTableEntries(tableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(updatedEntry), super.authHelper.retrieveMasterToken(), 0L);
        return reply.join().get(0).getSegmentVersion();
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

