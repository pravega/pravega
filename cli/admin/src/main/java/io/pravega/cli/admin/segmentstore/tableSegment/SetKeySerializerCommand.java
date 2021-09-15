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
import io.pravega.client.stream.Serializer;
import org.apache.curator.shaded.com.google.common.base.Charsets;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SetKeySerializerCommand extends TableSegmentCommand {
    private static final Map<String, Serializer<String>> SERIALIZERS = ImmutableMap.<String, Serializer<String>>builder()
            .put("slts_key", new Serializer<>() {
                @Override
                public ByteBuffer serialize(String value) {
                    return ByteBuffer.wrap(value.getBytes(Charsets.UTF_8));
                }

                @Override
                public String deserialize(ByteBuffer serializedValue) {
                    return StandardCharsets.UTF_8.decode(serializedValue).toString();
                }
            })
            .put("container_meta_key", new Serializer<>() {
                @Override
                public ByteBuffer serialize(String value) {
                    return ByteBuffer.wrap(value.getBytes(Charsets.UTF_8));
                }

                @Override
                public String deserialize(ByteBuffer serializedValue) {
                    return StandardCharsets.UTF_8.decode(serializedValue).toString();
                }
            })
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

        String identifier = getArg(0);
        getCommandArgs().getState().setKeySerializer(SERIALIZERS.get(identifier.toLowerCase()));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "set-key-serializer", "Set the serializer for the keys used in table segments.",
                new ArgDescriptor("serializer-name", "The required serializer. " +
                        "Serializer-names for built-in serializers are \"SLTS_KEY\"(for SLTS segments) and \"CONTAINER_META_KEY\"(for container segments)."));
    }
}
