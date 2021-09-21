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
import io.pravega.cli.admin.serializers.AbstractSerializer;
import io.pravega.cli.admin.serializers.ContainerKeySerializer;
import io.pravega.cli.admin.serializers.SltsKeySerializer;

import java.util.Map;

public class SetKeySerializerCommand extends TableSegmentCommand {
    private static final Map<String, AbstractSerializer> SERIALIZERS = ImmutableMap.<String, AbstractSerializer>builder()
            .put("slts_key", new SltsKeySerializer())
            .put("container_meta_key", new ContainerKeySerializer())
            .build();

    /**
     * Creates a new instance of the SetKeySerializerCommand.
     *
     * @param args The arguments for the command.
     */
    public SetKeySerializerCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);

        String identifier = getArg(0).toLowerCase();
        if (!SERIALIZERS.containsKey(identifier)) {
            output("Key serializer named %s does not exist.", identifier);
        } else {
            getCommandArgs().getState().setKeySerializer(SERIALIZERS.get(identifier));
            output("Key serializer changed to %s successfully.", identifier);
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "set-key-serializer", "Set the serializer for the keys used in table segments.",
                new ArgDescriptor("serializer-name", "The required serializer. " +
                        "Serializer-names for built-in serializers are " + SERIALIZERS.keySet().stream().reduce("", (sList, s) -> sList + ", " + s) + "."));
    }
}
