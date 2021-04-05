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
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;

/**
 * Updates a set of attributes on a segment.
 */
public class UpdateSegmentAttributesCommand extends StorageCommand {
    public UpdateSegmentAttributesCommand(CommandArgs args) {
        super(args);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "update-attributes", "Updates a set of attributes for a segment.",
                new ArgDescriptor("segment-name", "Fully qualified segment name (include scope and stream)"),
                new ArgDescriptor("list-of-attribute-updates", "Space-separated list of attribute ids to values to update " +
                        "(i.e., 'attribute1=value1 attribute2=value2 attribute3=value3' (value should be omitted for removals)."));
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2, 1000);

        final String segmentName = getArg(0);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(segmentName), "Invalid segment name");

        val updatedValues = new HashMap<UUID, Long>();
        output("These attributes are about to be updated:");
        for (int i = 1; i < getArgCount(); i++) {
            Map.Entry<UUID, Long> e = getArg(i, a -> {
                int pos = a.lastIndexOf('=');
                Preconditions.checkArgument(pos > 0, "Expected format of attribute=value");
                val attributeId = UUID.fromString(a.substring(0, pos));
                val value = (pos == a.length() - 1) ? null : Long.parseLong(a.substring(pos + 1));
                return new AbstractMap.SimpleImmutableEntry<>(attributeId, value);
            });
            updatedValues.put(e.getKey(), e.getValue());
            output("\t %s: %s", e.getKey(), e.getValue());
        }

        if (!confirmContinue()) {
            return;
        }

        @Cleanup
        val storage = this.storageFactory.createStorageAdapter();
        storage.initialize(Integer.MAX_VALUE);
        @Cleanup
        val segment = new DebugStorageSegment(segmentName, storage, executorService());

        segment.updateAttributes(updatedValues).get(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        output("Values updated successfully.");
    }
}
