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

import com.google.common.collect.ImmutableMap;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class GetTableSegmentInfoCommand extends TableSegmentCommand {

    public static final String SEGMENT_NAME = "segmentName";
    public static final String START_OFFSET = "startOffset";
    public static final String LENGTH = "length";
    public static final String ENTRY_COUNT = "entryCount";
    public static final String KEY_LENGTH = "keyLength";

    private static final Map<String, Function<WireCommands.TableSegmentInfo, Object>> SEGMENT_INFO_FIELD_MAP =
            ImmutableMap.<String, Function<WireCommands.TableSegmentInfo, Object>>builder()
                    .put(SEGMENT_NAME, WireCommands.TableSegmentInfo::getSegmentName)
                    .put(START_OFFSET, WireCommands.TableSegmentInfo::getStartOffset)
                    .put(LENGTH, WireCommands.TableSegmentInfo::getLength)
                    .put(ENTRY_COUNT, WireCommands.TableSegmentInfo::getEntryCount)
                    .put(KEY_LENGTH, WireCommands.TableSegmentInfo::getKeyLength)
                    .build();

    /**
     * Creates a new instance of the GetTableSegmentInfoCommand.
     *
     * @param args The arguments for the command.
     */
    public GetTableSegmentInfoCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);

        final String fullyQualifiedTableSegmentName = getArg(0);
        final String segmentStoreHost = getArg(1);
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        CompletableFuture<WireCommands.TableSegmentInfo> reply = adminSegmentHelper.getTableSegmentInfo(fullyQualifiedTableSegmentName,
                new PravegaNodeUri(segmentStoreHost, getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());

        WireCommands.TableSegmentInfo tableSegmentInfo = reply.join();
        output("TableSegmentInfo for %s: ", fullyQualifiedTableSegmentName);
        SEGMENT_INFO_FIELD_MAP.forEach((name, f) -> output("%s = %s", name, f.apply(tableSegmentInfo)));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-info", "Get the details of a given table.",
                new ArgDescriptor("qualified-table-segment-name", "Fully qualified name of the table to get info from."),
                new ArgDescriptor("segmentstore-endpoint", "Address of the Segment Store we want to send this request."));
    }
}
