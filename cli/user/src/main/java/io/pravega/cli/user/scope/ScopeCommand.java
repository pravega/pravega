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
package io.pravega.cli.user.scope;

import io.pravega.cli.user.Command;
import io.pravega.cli.user.CommandArgs;
import io.pravega.cli.user.stream.StreamCommand;
import io.pravega.client.admin.StreamManager;
import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;
import java.util.ArrayList;
import java.util.Collections;

public abstract class ScopeCommand extends Command {
    static final String COMPONENT = "scope";

    public ScopeCommand(@NonNull CommandArgs commandArgs) {
        super(commandArgs);
    }

    private static Command.CommandDescriptor.CommandDescriptorBuilder createDescriptor(String name, String description) {
        return Command.CommandDescriptor.builder()
                .component(COMPONENT)
                .name(name)
                .description(description);
    }

    public static class Create extends StreamCommand {
        public Create(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            @Cleanup
            val sm = StreamManager.create(getClientConfig());
            for (val scopeName : getCommandArgs().getArgs()) {
                val success = sm.createScope(scopeName);
                if (success) {
                    output("Scope '%s' created successfully.", scopeName);
                } else {
                    output("Scope '%s' could not be created.", scopeName);
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("create", "Creates one or more Scopes.")
                    .withArg("scope-names", "Name of the Scopes to create.")
                    .build();
        }
    }

    public static class Delete extends StreamCommand {
        public Delete(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureMinArgCount(1);
            @Cleanup
            val sm = StreamManager.create(getClientConfig());
            for (val scopeName : getCommandArgs().getArgs()) {
                val success = sm.deleteScope(scopeName);
                if (success) {
                    output("Scope '%s' deleted successfully.", scopeName);
                } else {
                    output("Scope '%s' could not be deleted.", scopeName);
                }
            }
        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("delete", "Deletes one or more Scopes.")
                    .withArg("scope-names", "Names of the Scopes to delete.")
                    .build();
        }
    }

    public static class List extends StreamCommand {
        public List(@NonNull CommandArgs commandArgs) {
            super(commandArgs);
        }

        @Override
        public void execute() {
            ensureArgCount(0);
            @Cleanup
            val sm = StreamManager.create(getClientConfig());
            val scopeIterator = sm.listScopes();
            ArrayList<String> scopeList = new ArrayList<>();

            while (scopeIterator.hasNext()) {
                scopeList.add(scopeIterator.next());
            }

            Collections.sort(scopeList);

            for (String scope : scopeList) {
                output("\t%s", scope);
            }

        }

        public static CommandDescriptor descriptor() {
            return createDescriptor("list", "Lists all Scopes in Pravega.")
                    .build();
        }
    }
}
