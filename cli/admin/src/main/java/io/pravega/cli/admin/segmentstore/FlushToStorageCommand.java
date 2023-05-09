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
package io.pravega.cli.admin.segmentstore;

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.cli.admin.utils.ZKHelper;
import io.pravega.common.cluster.Host;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Cleanup;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.curator.framework.CuratorFramework;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.lang.Integer.parseInt;

/**
 * Executes a FlushToStorage request against the chosen Segment Store instance.
 */
public class FlushToStorageCommand extends ContainerCommand {

    private static final int REQUEST_TIMEOUT_SECONDS = 60 * 30;
    private static final String ALL_CONTAINERS = "all";

    /**
     * Creates new instance of the FlushToStorageCommand.
     *
     * @param args The arguments for the command.
     */
    public FlushToStorageCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() throws Exception {
        validateArguments();
        final String containerId = getArg(0);
        int startContainerId;
        int endContainerId;
        @Cleanup
        CuratorFramework zkClient = createZKClient();
        @Cleanup
        AdminSegmentHelper adminSegmentHelper = instantiateAdminSegmentHelper(zkClient);
        if (containerId.equalsIgnoreCase(ALL_CONTAINERS)) {
            startContainerId = 0;
            endContainerId = getServiceConfig().getContainerCount() - 1;
        } else {
            startContainerId = parseInt(containerId);
            endContainerId = getArgCount() == 2 ? parseInt(getArg(1)) : startContainerId;
        }

        for (int id = startContainerId; id <= endContainerId; id++) {
            flushContainerToStorage(adminSegmentHelper, id);
        }
    }

    private void flushContainerToStorage(AdminSegmentHelper adminSegmentHelper, int containerId) throws Exception {
        CompletableFuture<WireCommands.StorageFlushed> reply = adminSegmentHelper.flushToStorage(containerId,
                new PravegaNodeUri(this.getHostByContainer(containerId), getServiceConfig().getAdminGatewayPort()), super.authHelper.retrieveMasterToken());
        reply.get(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        output("Flushed the Segment Container with containerId %d to Storage.", containerId);
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "flush-to-storage", "Persist the given Segment Container into Storage.",
                new ArgDescriptor("start-container-id", "The start container Id of the Segment Container that needs to be persisted, " +
                        "if given as \"all\" all the containers will be persisted. If given as container id without "), new ArgDescriptor("end-container-id", "The end container Id of the Segment Container that needs to be persisted, " +
                "This is an optional parameter. If not given then only start container id will be flushed"));
    }

    private String getHostByContainer(int containerId) {
        String host = this.getHosts().get(containerId);
        if (host == null || host.isEmpty()) {
            throw new RuntimeException("No host found for given container: " + containerId);
        }
        return host;
    }

    private Map<Integer, String> getHosts() {
        Map<Host, Set<Integer>> hostMap;
        try {
            @Cleanup
            ZKHelper zkStoreHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            hostMap = zkStoreHelper.getCurrentHostMap();
        } catch (Exception e) {
            throw new RuntimeException("Error getting segment store hosts for containers: " + e.getMessage());
        }

        if (hostMap == null || hostMap.isEmpty()) {
            throw new RuntimeException("Error getting segment store hosts for containers.");
        }

        Map<Integer, String> containerHostMap = new HashMap<>();
        for (Map.Entry<Host, Set<Integer>> entry: hostMap.entrySet()) {
            String ipAddr = entry.getKey().getIpAddr();
            Set<Integer> containerIds = entry.getValue();
            for (Integer containerId : containerIds) {
                containerHostMap.put(containerId, ipAddr);
            }
        }
        return containerHostMap;
    }

    private void validateArguments() {
        Preconditions.checkArgument(getArgCount() > 0, "Incorrect argument count.");
        final String container = getArg(0);
        if (!NumberUtils.isNumber(container)) {
            Preconditions.checkArgument(container.equalsIgnoreCase("all"), "Container argument should either be ALL/all or a container id.");
            Preconditions.checkArgument(getArgCount() == 1, "Incorrect argument count.");
        } else {
            final int startContainer = Integer.parseInt(container);
            final int containerCount = getServiceConfig().getContainerCount();
            Preconditions.checkArgument(startContainer < containerCount, "The start container id does not exist. There are %s containers present", containerCount);
            Preconditions.checkArgument(startContainer >= 0, "The start container id must be a positive number.");

            if (getArgCount() != 1) {
                Preconditions.checkArgument(NumberUtils.isNumber(getArg(1)), "End container id must be a number.");
                Preconditions.checkArgument(getArgCount() == 2, "Incorrect argument count.");
                final int endContainerId = Integer.parseInt(getArg(1));
                Preconditions.checkArgument(endContainerId < containerCount, "The end container id does not exist. There are %s containers present", containerCount);
                Preconditions.checkArgument(endContainerId >= 0, "The end container  id must be a positive number.");
                Preconditions.checkArgument(startContainer <= endContainerId, "End container id must be greater than or equal to start container id.");

            }
        }
    }
}
