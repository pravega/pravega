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
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;

/**
 * Gets a set of Attributes for a Segment (based on Attribute Id).
 */
public class GetSegmentAttributesCommand extends StorageCommand {
    public GetSegmentAttributesCommand(CommandArgs args) {
        super(args);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-attributes", "Gets a set of attributes for a segment.",
                new ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"),
                new ArgDescriptor("list-of-attribute-ids", "Space-separated list of attribute ids to retrieve."));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2, 1000);

        final String segmentName = getArg(0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");

        val attributeIds = new ArrayList<UUID>();
        for (int i = 1; i < getArgCount(); i++) {
            attributeIds.add(getArg(i, UUID::fromString));
        }

        @Cleanup
        val storage = this.storageFactory.createStorageAdapter();
        storage.initialize(Integer.MAX_VALUE);
        @Cleanup
        val segment = new DebugStorageSegment(segmentName, storage, executorService());

        val result = segment.getAttributes(attributeIds).get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        output("Listing %s/%s attribute(s):", result.size(), attributeIds.size());
        for (val e : result.entrySet()) {
            output("\t%s", formatAttribute(e));
        }
    }
}
