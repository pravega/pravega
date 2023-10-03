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
import io.pravega.cli.admin.serializers.ContainerMetadataSerializer;
import io.pravega.cli.admin.serializers.SltsKeySerializer;
import io.pravega.cli.admin.serializers.SltsMetadataSerializer;
import org.apache.commons.lang3.tuple.ImmutablePair;

import java.util.Map;

public class SetSerializerCommand extends TableSegmentCommand {
    private static final Map<String, ImmutablePair<AbstractSerializer, AbstractSerializer>> SERIALIZERS =
            ImmutableMap.<String, ImmutablePair<AbstractSerializer, AbstractSerializer>>builder()
                    .put("slts", ImmutablePair.of(new SltsKeySerializer(), new SltsMetadataSerializer()))
                    .put("container_meta", ImmutablePair.of(new ContainerKeySerializer(), new ContainerMetadataSerializer()))
                    .build();

    /**
     * Creates a new instance of the SetSerializerCommand.
     *
     * @param args The arguments for the command.
     */
    public SetSerializerCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);

        String identifier = getArg(0).toLowerCase();
        ImmutablePair<AbstractSerializer, AbstractSerializer> serializer = SERIALIZERS.get(identifier);
        if ( serializer == null ) {
            output("Serializers named %s do not exist.", identifier);
        } else {
            getCommandArgs().getState().setKeySerializer(serializer.getLeft());
            getCommandArgs().getState().setValueSerializer(serializer.getRight());
            output("Serializers changed to %s successfully.", identifier);
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "set-serializer", "Set the serializer for keys and values that are obtained from, and updated to table segments.",
                new ArgDescriptor("serializer-name", "The required serializer. " +
                        "Serializer-names for built-in serializers are " + String.join(",", SERIALIZERS.keySet())));
    }
}
