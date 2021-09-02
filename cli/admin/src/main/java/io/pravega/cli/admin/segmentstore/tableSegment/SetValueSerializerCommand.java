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
import io.pravega.cli.admin.serializers.StorageData;
import io.pravega.client.stream.Serializer;

import java.util.Map;

public class SetValueSerializerCommand extends TableSegmentCommand {
    private static final Map<String, Serializer> SERIALIZERS = ImmutableMap.<String, Serializer>builder()
            .put("slts_value", new StorageData.StorageDataSerializer())
            .build();

    /**
     * Creates a new instance of the SetKeySerializerCommand.
     *
     * @param args The arguments for the command.
     */
    public SetValueSerializerCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);

        String identifier = getArg(0);
        getCommandArgs().getState().setValueSerializer(SERIALIZERS.get(identifier.toLowerCase()));
    }
}
