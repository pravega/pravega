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
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.containers.DebugStorageSegment;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import lombok.val;

/**
 * Scans a Table Segment for Table Entries (valid or invalid).
 */
public class ScanTableEntriesCommand extends StorageCommand {
    private static final int MAX_AT_ONCE = 10000;

    public ScanTableEntriesCommand(CommandArgs args) {
        super(args);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "scan-table-entries", "Scans a table entries for entries.",
                new ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"),
                new ArgDescriptor("start-offset", "Offset within the table segment to start scanning at"),
                new ArgDescriptor("max-length", "Maximum number of bytes to scan"),
                new ArgDescriptor("output-file-path", "[Optional] Path to (local) file where to write the result"));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(3, 4);

        final String segmentName = getArg(0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");
        final long startOffset = getLongArg(1);
        Preconditions.checkArgument(startOffset >= 0, "start-offset must be non-negative");
        final int maxLength = getIntArg(2);
        Preconditions.checkArgument(maxLength > 0, "max-length must be a positive integer");

        final String targetPath = getCommandArgs().getArgs().size() < 4 ? null : getArg(3);

        @Cleanup
        val storage = this.storageFactory.createStorageAdapter();
        storage.initialize(Integer.MAX_VALUE);
        @Cleanup
        val segment = new DebugStorageSegment(segmentName, storage, executorService());

        @Cleanup
        val writer = targetPath == null ? new ConsoleWriter(MAX_AT_ONCE) : new FileWriter(Paths.get(targetPath));
        if (!writer.initialize()) {
            return;
        }

        val totalCount = new AtomicLong(0);
        val validCount = new AtomicLong(0);
        val invalidCount = new AtomicLong(0);

        val iterator = segment.scanTableSegment(startOffset, maxLength).get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        while (iterator.hasNext()) {
            val entry = iterator.next();
            if (entry instanceof DebugStorageSegment.ValidTableEntryInfo) {
                validCount.incrementAndGet();
            } else {
                invalidCount.incrementAndGet();
            }
            totalCount.incrementAndGet();
            if (!writer.write(formatTableEntry(entry))) {
                break;
            }
        }

        output("Entries: %s, Valid: %s, Invalid: %s", totalCount, validCount, invalidCount);
    }
}