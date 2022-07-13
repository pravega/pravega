/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
 * Delete a reader group from scope.
 */
public class ControllerDeleteReaderGroupCommand extends ControllerCommand {
    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDeleteReaderGroupCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);
        @Cleanup
        val context = createContext();
        String scope = getArg(0);
        String readerGroup = getArg(1);
        output(executeDeleteRESTCall(context, "/v1/scopes/" + scope + "/readergroups/" + readerGroup));
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "delete-readergroup", "Delete a readergroup in a given Scope.",
                new ArgDescriptor("scope-name", "Name of the Scope where the readergroup is stored."),
                new ArgDescriptor("readergroup", "Name of the readergroup."));
    }
}
