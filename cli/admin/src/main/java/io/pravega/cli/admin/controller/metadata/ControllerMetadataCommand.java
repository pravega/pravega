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
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.client.tables.impl.TableSegmentEntry;
import io.pravega.client.tables.impl.TableSegmentKey;
import io.pravega.client.tables.impl.TableSegmentKeyVersion;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.controller.server.WireCommandFailedException;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.INTEGER;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.LONG;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.STRING;
import static io.pravega.cli.admin.serializers.controller.ControllerMetadataSerializer.EMPTY;

public abstract class ControllerMetadataCommand extends ControllerCommand {
    static final String COMPONENT = "controller-metadata";
    static final ControllerKeySerializer KEY_SERIALIZER = new ControllerKeySerializer();
    private static final String ROOT = "eventProcessors";
    private static final String HOSTREQUESTINDEX = "hostRequestIndex";

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
     * @param adminSegmentHelper An instance of {@link AdminSegmentHelper}.
     * @return The queried table segment entry.
     */
    TableSegmentEntry getTableEntry(String tableName, String key, String segmentStoreHost, AdminSegmentHelper adminSegmentHelper) {
        ByteArraySegment serializedKey = new ByteArraySegment(KEY_SERIALIZER.serialize(key));

        List<TableSegmentEntry> entryList = completeSafely(adminSegmentHelper.readTable(tableName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(TableSegmentKey.unversioned(serializedKey.getCopy())),
                authHelper.retrieveMasterToken(), 0L), tableName, key);
        if (entryList == null) {
            return null;
        }

        if (entryList.get(0).getKey().getVersion().equals(TableSegmentKeyVersion.NOT_EXISTS)) {
            output(String.format("Key not found: %s", key));
            return null;
        }
        return entryList.get(0);
    }

    /**
     * Method to update entry corresponding to the provided key in the table.
     *
     * @param tableName          The name of the table.
     * @param key                The key.
     * @param value              The new value.
     * @param version            The expected update version.
     * @param segmentStoreHost   The address of the segment store instance.
     * @param adminSegmentHelper An instance of {@link AdminSegmentHelper}.
     * @return The new key version after the update takes place successfully.
     */
    TableSegmentKeyVersion updateTableEntry(String tableName, String key, ByteBuffer value, long version, String segmentStoreHost, AdminSegmentHelper adminSegmentHelper) {
        ByteArraySegment serializedKey = new ByteArraySegment(KEY_SERIALIZER.serialize(key));
        ByteArraySegment serializedValue = new ByteArraySegment(value);

        TableSegmentEntry updatedEntry = TableSegmentEntry.versioned(serializedKey.getCopy(), serializedValue.getCopy(), version);
        List<TableSegmentKeyVersion> keyVersions = completeSafely(adminSegmentHelper.updateTableEntries(tableName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()),
                Collections.singletonList(updatedEntry), authHelper.retrieveMasterToken(), 0L), tableName, key);
        if (keyVersions == null) {
            return null;
        }

        return keyVersions.get(0);
    }

    /**
     * Method to safely complete a table query.
     *
     * @param future    The CompletableFuture containing the table query.
     * @param tableName The table name.
     * @param key       The key.
     * @param <T>       Type of the result of the table query.
     * @return The result of future.join() and null in case of an exception.
     */
    <T> T completeSafely(CompletableFuture<T> future, String tableName, String key) {
        try {
            return Futures.getThrowingException(future);
        } catch (WireCommandFailedException e) {
            switch (e.getReason()) {
                case SegmentDoesNotExist:
                    output(String.format("Table not found: %s", tableName));
                    break;
                case AuthFailed:
                    output("Authentication failed.");
                    break;
                case UnknownHost:
                    output("Unknown host provided. Retry with the correct segment store address.");
                    break;
                case TableKeyDoesNotExist:
                    output("Key not found: %s", key);
                    break;
                case TableKeyBadVersion:
                    output("Update failed due to incorrect key version. " +
                            "This indicates that the record has been updated since you last read it, please try again.");
                    break;
                default:
                    output("Something unexpected happened.");
                    output(e.getMessage());
            }
            return null;
        } catch (Exception e) {
            output("Something unexpected happened.");
            output(e.getMessage());
            return null;
        }
    }

    /**
     * Print the provided data in a user-friendly manner.
     *
     * @param data The data to be printed.
     * @param name A name describing the data.
     */
    void userFriendlyOutput(String data, String name) {
        switch (name) {
            case STRING:
            case INTEGER:
            case LONG:
                output("value: %s", data);
                return;
            case EMPTY:
                output("value: None");
                return;
            default:
                output("%s metadata info: ", name);
                output(data);
        }
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


    /**
     * Method to get path of reader in a particular controller instance.
     *
     * @param hostId Host id of controller instance
     * @param readerGroup name of the readerGroup
     * @param readerId Id of reader
     * @return Full path of reader
     */
    protected String getReaderPath(String hostId, String readerGroup, String readerId) {
        return String.format("/%s/%s/%s/%s", ROOT, hostId, readerGroup, readerId);
    }

    /**
     * Method to get path of request in a particular controller instance.
     *
     * @param hostId Host id of controller instance.
     * @param requestId Request UUID.
     * @return Full path of request.
     */
    protected String getRequestPath(String hostId, String requestId) {
        return String.format("/%s/%s/%s", HOSTREQUESTINDEX, hostId, requestId);
    }
}
