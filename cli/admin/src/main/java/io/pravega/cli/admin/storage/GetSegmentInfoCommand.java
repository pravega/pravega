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
package io.pravega.cli.admin.storage;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.containers.DebugStorageSegment;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;

/**
 * Gets information about a segment (it can be a main segment or attribute segment - as long as it's a valid LTS segment).
 */
public class GetSegmentInfoCommand extends StorageCommand {
    public GetSegmentInfoCommand(CommandArgs args) {
        super(args);
    }

    public static AdminCommand.CommandDescriptor descriptor() {
        return new AdminCommand.CommandDescriptor(COMPONENT, "segment-info", "Gets information about a segment.",
                new AdminCommand.ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1);
        final String segmentName = getArg(0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");

        @Cleanup
        val storage = this.storageFactory.createStorageAdapter();
        storage.initialize(Integer.MAX_VALUE);
        @Cleanup
        val segment = new DebugStorageSegment(segmentName, storage, executorService());

        val info = segment.getSegmentInfo().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        output("Segment Name: %s", info.getName());
        output("Length      : %s", info.getLength());
        output("Start Offset: %s", info.getStartOffset());
        output("Sealed      : %s", info.isSealed());
        output("Deleted     : %s", info.isDeleted());
        output("Modified    : %s", info.getLastModified().asDate());
        output("Attributes  : %s", info.getAttributes().size());
    }
}