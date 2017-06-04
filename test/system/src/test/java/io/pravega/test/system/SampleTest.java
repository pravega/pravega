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

import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import io.pravega.test.system.framework.SystemTestRunner;
import io.pravega.test.system.framework.docker.Dockerclient;
import io.pravega.test.system.framework.services.ZookeeperDockerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.net.URI;

import static org.apache.curator.framework.imps.CuratorFrameworkState.STARTED;
import static org.junit.Assert.assertEquals;

@Slf4j
@RunWith(SystemTestRunner.class)
public class SampleTest  {

    @Before
    public static void setup() throws Exception {

        DockerClient docker = Dockerclient.getClient();
        docker.pull("java:8");
        final ContainerConfig containerConfig = ContainerConfig.builder()
                .image("java:8")
                .build();
        final ContainerCreation creation = docker.createContainer(containerConfig);
        final String id = creation.id();

        final ContainerInfo info = docker.inspectContainer(id);
        docker.startContainer(id);

        ZookeeperDockerService zookeeperDockerService = new ZookeeperDockerService("zookeeper");
        if (!zookeeperDockerService.isRunning()) {
            zookeeperDockerService.start(true);
        }
    }

    @Test
    public void zkTest() throws DockerCertificateException, InterruptedException, DockerException {

        log.info("Start execution of ZkTest");
        String zk = "zookeeper:2181";
        CuratorFramework curatorFrameworkClient =
                CuratorFrameworkFactory.newClient(zk, new RetryOneTime(5000));
        curatorFrameworkClient.start();
        log.info("CuratorFramework status {} ", curatorFrameworkClient.getState());
        assertEquals("Connection to zk client ", STARTED, curatorFrameworkClient.getState());
        log.info("Zookeeper Service URI : {} ", URI.create(zk));
        log.info("ZkTest  execution completed");
    }

}
