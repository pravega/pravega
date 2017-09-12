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
import com.spotify.docker.client.messages.swarm.Node;
import com.spotify.docker.client.messages.swarm.SwarmInit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Client {

    public static DockerClient getDockerClient() {
      return createDockerClient();
    }

    private static DockerClient createDockerClient() {

        DockerClient dockerClient = DefaultDockerClient.builder().uri("http://localhost:2375").build();
        try {
            //check if the node is already in swarm
            Node node = dockerClient.listNodes().get(0);
            if (!(node.description().hostname().equals("ubuntu"))) {
                log.info("This node is not in swarm");
                dockerClient.initSwarm(SwarmInit.builder()
                        .advertiseAddr("127.0.0.1")
                        .listenAddr("0.0.0.0:2377")
                        .build()
                );
            }
            if (dockerClient.listNetworks(DockerClient.ListNetworksParam.byNetworkName("network-name")).isEmpty()) {
                dockerClient.createNetwork(NetworkConfig.builder().driver("overlay").name("network-name").build());
            }
        } catch (DockerException | InterruptedException e) {
            log.error("unable to create network {}", e);
        }

        return dockerClient;
    }
}
