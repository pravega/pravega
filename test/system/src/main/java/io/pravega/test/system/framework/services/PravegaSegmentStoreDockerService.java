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

import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ServiceCreateResponse;
import com.spotify.docker.client.messages.mount.Mount;
import com.spotify.docker.client.messages.swarm.ContainerSpec;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
import com.spotify.docker.client.messages.swarm.NetworkAttachmentConfig;
import com.spotify.docker.client.messages.swarm.PortConfig;
import com.spotify.docker.client.messages.swarm.ResourceRequirements;
import com.spotify.docker.client.messages.swarm.Resources;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static io.pravega.test.system.framework.services.PravegaControllerDockerService.CONTROLLER_PORT;
import static io.pravega.test.system.framework.services.ZookeeperDockerService.ZKSERVICE_ZKPORT;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class PravegaSegmentStoreDockerService extends DockerBasedService {

    private static final int SEGMENTSTORE_PORT = 12345;
    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 1000.0;

    public PravegaSegmentStoreDockerService(final String serviceName ) {
        super(serviceName);
    }

    @Override
    public void stop() {
        try {
                docker.removeService(getID());
        } catch (DockerException | InterruptedException e) {
            log.error("unable to remove service {}", e);
        }
    }

    @Override
    public void clean() {
    }

    @Override
    public void start(final boolean wait) {
        try {
            ServiceCreateResponse serviceCreateResponse = docker.createService(setServiceSpec());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        }  catch (InterruptedException | DockerException | TimeoutException | ExecutionException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {
        Mount mount = Mount.builder().type("volume").source("logs-volume").target("/tmp/logs").build();
        //set env
        String zk = "zookeeper" + ":" + ZKSERVICE_ZKPORT;
        String con = "controller" + ":" + CONTROLLER_PORT;
        //System properties to configure SS service.
        String hostSystemProperties = setSystemProperty("pravegaservice.zkURL", zk) +
                setSystemProperty("bookkeeper.zkAddress", zk) +
                setSystemProperty("hdfs.hdfsUrl", "hdfs:8020") +
                setSystemProperty("autoScale.muteInSeconds", "120") +
                setSystemProperty("autoScale.cooldownInSeconds", "120") +
                setSystemProperty("autoScale.cacheExpiryInSeconds", "120") +
                setSystemProperty("autoScale.cacheCleanUpInSeconds", "120") +
                setSystemProperty("autoScale.controllerUri", con) +
                setSystemProperty("log.level", "DEBUG");
        String env1 = "PRAVEGA_SEGMENTSTORE_OPTS="+hostSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx900m";
        List<String> stringList =  new ArrayList<>();
        stringList.add(env1);
        stringList.add(env2);
        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image("asdrepo.isus.emc.com:8103" + "/nautilus/pravega:0.1.0-1415.b5f03f5")
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L,  3))
                        .mounts(Arrays.asList(mount))
                        .env(stringList).args("segmentstore").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec =  ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target("network-name").build())
                .endpointSpec(EndpointSpec.builder().addPort(PortConfig.builder().
                        targetPort(SEGMENTSTORE_PORT).protocol("TCP").build())
                        .build()).build();
        return spec;
    }

    public static void main(String[] args) {
        ZookeeperDockerService zookeeperDockerService = new ZookeeperDockerService("zookeeper");
        if (!zookeeperDockerService.isRunning()) {
            zookeeperDockerService.start(true);
        }
        List<URI> zkUris = zookeeperDockerService.getServiceDetails();
        BookkeeperDockerService bookkeeperDockerService = new BookkeeperDockerService("bookkeeper");
        log.debug("zk uri details {}", zkUris.get(0).toString());
        if (!bookkeeperDockerService.isRunning()) {
            bookkeeperDockerService.start(true);
        }

        HdfsDockerService hdfsDockerService = new HdfsDockerService("hdfs");
        if (!hdfsDockerService.isRunning()) {
            hdfsDockerService.start(true);
        }

        PravegaControllerDockerService pravegaControllerDockerService = new PravegaControllerDockerService("controller");
        if (!pravegaControllerDockerService.isRunning()) {
            pravegaControllerDockerService.start(true);
        }
        List<URI> uriList = pravegaControllerDockerService.getServiceDetails();
        PravegaSegmentStoreDockerService pravegaSegmentStoreDockerService = new PravegaSegmentStoreDockerService("segmentstore");
        if (!pravegaSegmentStoreDockerService.isRunning()) {
            pravegaSegmentStoreDockerService.start(true);
        }
    }
}
