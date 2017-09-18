/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.docker;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.NetworkConfig;
import com.spotify.docker.client.messages.swarm.SwarmInit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Client {

    static final DockerClient DOCKER_CLIENT = DefaultDockerClient.builder().uri("http://127.0.0.1:2375").build();

    public static DockerClient getDockerClient() {
      return createDockerClient();
    }

    private static DockerClient createDockerClient() {
        try {
            log.info("This node is not in swarm");
            DOCKER_CLIENT.initSwarm(SwarmInit.builder()
                        .advertiseAddr("127.0.0.1")
                        .listenAddr("0.0.0.0:2377")
                        .build()
                );
            if (DOCKER_CLIENT.listNetworks(DockerClient.ListNetworksParam.byNetworkName("network-name")).isEmpty()) {
                DOCKER_CLIENT.createNetwork(NetworkConfig.builder().driver("overlay").name("network-name").build());
            }
        } catch (DockerException | InterruptedException e) {
            log.error("unable to create network {}", e);
        }
        return DOCKER_CLIENT;
    }
}
