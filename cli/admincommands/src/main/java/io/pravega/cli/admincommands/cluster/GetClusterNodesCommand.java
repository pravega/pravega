/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admincommands.cluster;

import io.pravega.cli.admincommands.CommandArgs;
import io.pravega.cli.admincommands.utils.ZKHelper;
import lombok.Cleanup;

/**
 * Outputs all the Controller, Segment Store and Bookkeeper instances in the system.
 */
public class GetClusterNodesCommand extends ClusterCommand {

    public GetClusterNodesCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(0);

        try {
            @Cleanup
            ZKHelper zkStoreHelper = ZKHelper.create(getServiceConfig().getZkURL(), getServiceConfig().getClusterName());
            prettyJSONOutput("cluster_name", getServiceConfig().getClusterName());
            prettyJSONOutput("controllers", zkStoreHelper.getControllers());
            prettyJSONOutput("segment_stores", zkStoreHelper.getSegmentStores());
            prettyJSONOutput("bookies", zkStoreHelper.getBookies());
        } catch (Exception e) {
            System.err.println("Exception accessing to Zookeeper cluster metadata.");
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "list-instances", "Lists all nodes in the Pravega " +
                "cluster (Controllers, Segment Stores).");
    }
}
