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

import com.google.common.io.Resources;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.LogStream;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.ExecCreation;
import lombok.extern.slf4j.Slf4j;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.fail;

@Slf4j
public class SampleTest {
    public static void main(String[] args) throws Exception {
        // Create a client based on DOCKER_HOST and DOCKER_CERT_PATH env vars
        DockerClient dockerClient = DefaultDockerClient.builder().uri("http://127.0.0.1:2375").build();

        // Pull an image
        dockerClient.pull("busybox:ubuntu");

        // Create container with exposed ports
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .image("busybox:ubuntu")
                .cmd("sh", "-c", "while :; do sleep 1; done")
                .build();

        final ContainerCreation creation = dockerClient.createContainer(containerConfig);
        final String id = creation.id();

        final Path dockerDirectory = Paths.get(Resources.getResource(System.getProperty("user.dir")).toURI());
        try {
            dockerClient.copyToContainer(dockerDirectory, id, "/tmp");
        } catch (Exception e) {
            fail("error to copy files to container");
        }

        // Inspect container
        final ContainerInfo info = dockerClient.inspectContainer(id);

        // Start container
        dockerClient.startContainer(id);

        // Exec command inside running container with attached STDOUT and STDERR
        final String[] command = {"bash", "-c", "ls"};
        final ExecCreation execCreation = dockerClient.execCreate(
                id, command, DockerClient.ExecCreateParam.attachStdout(),
                DockerClient.ExecCreateParam.attachStderr());
        final LogStream output = dockerClient.execStart(execCreation.id());
        log.debug("Output of the execution {} ", output.readFully());


    }
        //final String execOutput = output.readFully();

        // Kill container
        //dockerClient.killContainer(id);
        // Remove container
        //dockerClient.removeContainer(id);
        // Close the docker client
        //dockerClient.close();
    }

