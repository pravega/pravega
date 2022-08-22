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
package io.pravega.cli.admin.controller.metadata;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.ZKHelper;

/**
 * To view pending event detail for a request in a particular controller host.
 * When Controller submits events for processing various operations like updating s stream, creating a KVT, a reader Group etc.
 * these events are also added to a zookeeper znode belonging to that particular Controller host with path.
 * Controller events in this znode indicates the pending events that controller has not yet processed.
 */
public class ControllerMetadataViewPendingEventsCommand extends ControllerMetadataCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerMetadataViewPendingEventsCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        ensureArgCount(2);
        final String hostId = getArg(0);
        final String requestUuid = getArg(1);
        try {
            ZKHelper zkHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            output("request-detail: \n %s", zkHelper.getPendingEventsForRequest(getRequestPath(hostId, requestUuid)));
        } catch (Exception e) {
            output("Exception accessing pending events metadata : " + e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "request-detail", "Get the pending event detail for a request in a particular controller host.",
                new ArgDescriptor("host-id", "Id of controller host"),
                new ArgDescriptor("request-id", "Request UUID"));
    }
}
