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
import com.spotify.docker.client.messages.mount.VolumeOptions;
import com.spotify.docker.client.messages.swarm.ContainerSpec;
import com.spotify.docker.client.messages.swarm.EndpointSpec;
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
import static io.pravega.test.system.framework.services.MarathonBasedService.IMAGE_PATH;
import static io.pravega.test.system.framework.services.MarathonBasedService.PRAVEGA_VERSION;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class PravegaSegmentStoreDockerService extends DockerBasedService {

    private static final int SEGMENTSTORE_PORT = 12345;
    private final URI zkUri;
    private final URI conUri;
    private String serviceId;
    private int instances = 1;
    private double cpu = 0.1 * Math.pow(10.0, 9.0);
    private long mem = 1000 * 1024 * 1024L;

    public PravegaSegmentStoreDockerService(final String serviceName, final URI zkUri, final URI conUri ) {

        super(serviceName);
        this.zkUri = zkUri;
        this.conUri = conUri;
    }

    @Override
    public void stop() {
        try {
           docker.removeService(this.serviceId);
        }  catch (DockerException | InterruptedException e) {
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
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        }  catch (InterruptedException | DockerException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {

        Mount mount = Mount.builder().type("Volume").source("/mnt/logs").target("/tmp/logs").volumeOptions(VolumeOptions.builder().build()).readOnly(false).build();

        //set env
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;

        //System properties to configure SS service.
        String hostSystemProperties = setSystemProperty("pravegaservice.zkURL", zk) +
                setSystemProperty("bookkeeper.zkAddress", zk) +
                setSystemProperty("hdfs.hdfsUrl", "hdfs.marathon.containerip.dcos.thisdcos.directory:8020") +
                setSystemProperty("autoScale.muteInSeconds", "120") +
                setSystemProperty("autoScale.cooldownInSeconds", "120") +
                setSystemProperty("autoScale.cacheExpiryInSeconds", "120") +
                setSystemProperty("autoScale.cacheCleanUpInSeconds", "120") +
                setSystemProperty("autoScale.controllerUri", conUri.toString()) +
                setSystemProperty("log.level", "DEBUG");

        String env1 = "PRAVEGA_SEGMENTSTORE_OPTS="+hostSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx900m";

        List<String> stringList =  new ArrayList<>();
        stringList.add(env1);
        stringList.add(env2);

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + "/nautilus/pravega:" + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 30L, 3L, 3))
                        .mounts(Arrays.asList(mount))
                        .env(stringList).args("controller").build())
                .resources(ResourceRequirements.builder()
                        .limits(Resources.builder()
                                .memoryBytes(mem).nanoCpus((long) cpu).build())
                        .build())
                .build();

        ServiceSpec spec =  ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder().addPort(PortConfig.builder().
                        publishedPort(SEGMENTSTORE_PORT).targetPort(SEGMENTSTORE_PORT).protocol("TCP").build())
                        .build()).build();
        return spec;

    }
}
