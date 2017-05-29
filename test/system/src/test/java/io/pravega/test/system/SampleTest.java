/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ExecCreation;

public class SampleTest {

    public static void main(String[] args) throws Exception {
        // Create a client based on DOCKER_HOST and DOCKER_CERT_PATH env vars
        DockerClient docker = DefaultDockerClient.fromEnv().build();

        // Pull an image
        docker.pull("busybox:ubuntu");

        // Create container with exposed ports
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .image("busybox:ubuntu")
                .cmd("sh", "-c", "while :; do sleep 1; done")
                .build();

        final ContainerCreation creation = docker.createContainer(containerConfig);
        final String id = creation.id();

        // Inspect container
        final ContainerInfo info = docker.inspectContainer(id);

        // Start container
        docker.startContainer(id);

        // Exec command inside running container with attached STDOUT and STDERR
        final String[] command = {"bash", "-c", "ls"};
        final ExecCreation execCreation = docker.execCreate(
                id, command, DockerClient.ExecCreateParam.attachStdout(),
                DockerClient.ExecCreateParam.attachStderr());
        final LogStream output = docker.execStart(execCreation.id());
        //final String execOutput = output.readFully();

        // Kill container
        docker.killContainer(id);
        // Remove container
        docker.removeContainer(id);
        // Close the docker client
        docker.close();
    }
}