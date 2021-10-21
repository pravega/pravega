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
package io.pravega.cli.user.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.cli.user.Command;
import io.pravega.cli.user.CommandArgs;
import lombok.val;

import java.util.HashMap;

public abstract class ConfigCommand extends Command {

    static final String COMPONENT = "config";
    private static final String PAIR_SEPARATOR = "=";

    public ConfigCommand(CommandArgs args) {
        super(args);
    }

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    public static class Set extends ConfigCommand {
        public Set(CommandArgs args) {
            super(args);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            val newValues = new HashMap<String, String>();
            getCommandArgs().getArgs().forEach(s -> {
                String[] items = s.split(PAIR_SEPARATOR);
                Preconditions.checkArgument(items.length == 2, "Invalid name=value pair: '%s'.", s);
                Preconditions.checkArgument(!Strings.isNullOrEmpty(items[0]) && !Strings.isNullOrEmpty(items[1]),
                        "Invalid name=value pair: '%s'.", s);
                newValues.put(items[0], items[1]);
            });

            Preconditions.checkArgument(newValues.size() > 0, "Expecting at least one argument.");
            newValues.forEach(this.getCommandArgs().getConfig()::set);
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("set", "Sets one or more config values for use during this session.")
                    .withArg("name=value list", "Space-separated name=value pairs.")
                    .withSyntaxExample("config=value1 config2=value2", "Sets config1 to value1 and config2 to value2.")
                    .build();
        }
    }

    public static class List extends ConfigCommand {
        public List(CommandArgs args) {
            super(args);
        }

        @Override
        public void execute() {
            Preconditions.checkArgument(getCommandArgs().getArgs().size() == 0, "Not expecting any arguments.");
            getCommandArgs().getConfig().getAll().forEach((name, value) -> output("\t%s%s%s", name, PAIR_SEPARATOR, value));
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list", "Lists all configuration set during this session.").build();
        }
    }
}