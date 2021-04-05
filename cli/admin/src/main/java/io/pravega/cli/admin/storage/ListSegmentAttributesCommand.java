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
 * Lists all Attributes in a Segment's Attribute Index.
 */
public class ListSegmentAttributesCommand extends StorageCommand {
    private static final int MAX_AT_ONCE = 1000;

    public ListSegmentAttributesCommand(CommandArgs args) {
        super(args);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-attributes", "Lists all attributes for a segment (NOTE: may be a lot).",
                new ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"),
                new ArgDescriptor("output-file-path", "[Optional] Path to (local) file where to write the result"));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(1, 2);

        final String segmentName = getArg(0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");

        final String targetPath = getCommandArgs().getArgs().size() == 1 ? null : getArg(1);

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
        val iterator = segment.iterateAttributes().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        boolean canContinue = true;
        while (canContinue) {
            val next = iterator.getNext().get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (next == null) {
                break;
            }

            for (val attribute : next) {
                totalCount.incrementAndGet();
                if (!writer.write(formatAttribute(attribute))) {
                    canContinue = false;
                    break;
                }
            }
        }

        output("Found %s attribute(s).", totalCount);
    }
}
