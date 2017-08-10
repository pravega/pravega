/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.system.framework.docker;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DockerTeardown {

    public static void main(String[] args) {
        DockerClient dockerClient = Dockerclient.getClient();
        try {
            dockerClient.leaveSwarm(true);
            dockerClient.close();
        } catch (DockerException | InterruptedException e) {
            log.error("Unable to leave swarm: ", e);
        }

    }

}
