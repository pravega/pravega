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
package io.pravega.cli.admin.controller;

import io.pravega.cli.admin.CommandArgs;
import lombok.Cleanup;
import lombok.val;

/**
 * Provides details of a ReaderGroup in a given Scope.
 */
public class ControllerDescribeReaderGroupCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeReaderGroupCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);
        // Describe a the selected scope via REST API.
        @Cleanup
        val context = createContext();
        String scope = getArg(0);
        String readerGroup = getArg(1);
        prettyJSONOutput(executeRESTCall(context, "/v1/scopes/" + scope + "/readergroups/" + readerGroup));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-readergroup", "Get the details of a given ReaderGroup in a Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope where the ReaderGroup is stored."),
                new ArgDescriptor("readergroup-id", "Id of the ReaderGroup to describe."));
    }
}
