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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Dockerclient {

       public static DockerClient getClient() {
        return createDockerClient();
    }

    private static DockerClient createDockerClient() {
        DockerClient dockerClient = DefaultDockerClient.builder().uri("http://localhost:2375").build();
        try {
            dockerClient.createNetwork(NetworkConfig.builder().driver("overlay").name("network-name").build());
        } catch (DockerException | InterruptedException e) {
            log.error("unable to create network {}", e);
        }
        return dockerClient;
    }

}
