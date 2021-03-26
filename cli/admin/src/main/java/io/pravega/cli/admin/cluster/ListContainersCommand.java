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
import io.pravega.cli.admin.utils.ZKHelper;
import lombok.Cleanup;

/**
 * Outputs all the Segment Containers in the system and the Segment Stores responsible for them.
 */
public class ListContainersCommand extends ClusterCommand {

    public ListContainersCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(0);
        try {
            @Cleanup
            ZKHelper zkStoreHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            prettyJSONOutput("segment_store_container_map", zkStoreHelper.getCurrentHostMap());
        } catch (Exception e) {
            System.err.println("Exception accessing to Zookeeper cluster metadata: " + e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-containers", "Lists all the containers in the " +
                "Pravega cluster and the Segment Stores responsible for them.");
    }
}
