/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services;

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.NetworkConfig;
import com.spotify.docker.client.messages.swarm.SwarmInit;
import io.pravega.test.system.framework.docker.Dockerclient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DockerSwarm {

    public static void main(String[] args) {

        DockerClient dockerClient = Dockerclient.getClient();

       try {
           dockerClient.createNetwork(NetworkConfig.builder().driver("overlay").name("network-name").build());
           final String nodeId;
           nodeId = dockerClient.initSwarm(SwarmInit.builder()
                   .advertiseAddr("127.0.0.1")
                   .listenAddr("0.0.0.0:2377")
                   .build()
           );
       } catch (DockerException | InterruptedException e) {
           log.error("unable to initilaize a swarm {}", e);
       }

    }
}
