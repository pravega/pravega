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
package io.pravega.cli.admin.cluster;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.ZKConnectionFailedException;
import io.pravega.cli.admin.utils.ZKHelper;
import io.pravega.common.cluster.Host;
import java.util.Optional;
import lombok.Cleanup;

/**
 * Outputs the Segment Segment Store responsible for the given Container.
 */
public class GetSegmentStoreByContainerCommand extends ClusterCommand {

    public GetSegmentStoreByContainerCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(1);

        try {
            @Cleanup
            ZKHelper zkStoreHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            Optional<Host> host = zkStoreHelper.getHostForContainer(getIntArg(0));
            prettyJSONOutput("owner_segment_store", host.get());
        } catch (ZKConnectionFailedException e) {
            System.err.println("Exception accessing to Zookeeper cluster metadata.");
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "get-host-by-container", "Get the Segment Store host " +
                "responsible for a given container id.",
                new ArgDescriptor("container-id", "Id of the Container to get the associated Segment Store."));
    }
}
