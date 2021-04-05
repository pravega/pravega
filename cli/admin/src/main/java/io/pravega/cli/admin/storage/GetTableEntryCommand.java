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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.server.containers.DebugStorageSegment;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;

/**
 * Retrieves a single Table Entry from a Table Segment.
 */
public class GetTableEntryCommand extends StorageCommand {
    public GetTableEntryCommand(CommandArgs args) {
        super(args);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-table-entry", "Gets a single table entry for a segment.",
                new ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"),
                new ArgDescriptor("key", "The key (in UTF-8) string form."));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);

        final String segmentName = getArg(0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");

        val key = new ByteArraySegment(getArg(1).getBytes(Charsets.UTF_8));

        @Cleanup
        val storage = this.storageFactory.createStorageAdapter();
        storage.initialize(Integer.MAX_VALUE);
        @Cleanup
        val segment = new DebugStorageSegment(segmentName, storage, executorService());

        val result = segment.getTableEntry(key).get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        output("Result: %s", formatTableEntry(result));
    }
}