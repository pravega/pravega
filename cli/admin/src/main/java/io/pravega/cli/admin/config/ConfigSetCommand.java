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
package io.pravega.cli.admin.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import java.util.Properties;

import static io.pravega.cli.admin.utils.ConfigUtils.getIfEnv;

/**
 * Updates the shared AdminCommandState with new config values.
 */
public class ConfigSetCommand extends AdminCommand {
    /**
     * Creates a new instance of the ConfigSetCommand class.
     *
     * @param args The arguments for the command.
     */
    public ConfigSetCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        Properties newValues = new Properties();
        getCommandArgs().getArgs().forEach(s -> {
            String[] items = s.split("=");
            Preconditions.checkArgument(items.length == 2, "Invalid name=value pair: '%s'.", s);
            Preconditions.checkArgument(!Strings.isNullOrEmpty(items[0]) && !Strings.isNullOrEmpty(items[1]),
                    "Invalid name=value pair: '%s'.", s);
            newValues.setProperty(items[0], getIfEnv(items[1]));
        });

        Preconditions.checkArgument(newValues.size() > 0, "Expecting at least one argument.");
        getCommandArgs().getState().getConfigBuilder().include(newValues);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(ConfigCommand.COMPONENT, "set",
                "Sets one or more config values for use during this session. Environment variables can also be provided. " +
                        "For example, if the variable is ENV_VARIABLE, it can be provided as name=$ENV_VARIABLE.",
                new ArgDescriptor("name=value list", "Space-separated name=value pairs."));
    }
}
