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
import lombok.val;

/**
 * Gets a description of different characteristics related to a Stream (e.g., configuration, state, active txn).
 */
public class ControllerDescribeStreamCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeStreamCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);
        final String scope = getArg(0);
        final String stream = getArg(1);

        try (val context = createContext()) {
            prettyJSONOutput(executeRESTCall(context, "/v1/scopes/" + scope + "/streams/" + stream));
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-stream", "Get the details of a given Stream.",
                new ArgDescriptor("scope-name", "Name of the Scope where the Stream belongs to."),
                new ArgDescriptor("stream-name", "Name of the Stream to describe."));
    }
}
