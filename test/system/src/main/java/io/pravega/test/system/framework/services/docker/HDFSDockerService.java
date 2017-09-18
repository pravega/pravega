/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services.docker;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class HDFSDockerService extends DockerBasedService {

    private int instances = 1;
    private double cpu = 0.1;
    private double mem = 2048.0;

    public HDFSDockerService(final String serviceName) {
        super(serviceName);
    }

    @Override
    public void stop() {
        try {
            dockerClient.removeService(getID());
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
            ServiceCreateResponse serviceCreateResponse = dockerClient.createService(setServiceSpec());
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        } catch (InterruptedException | DockerException | TimeoutException | ExecutionException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {
        Mount mount = Mount.builder().type("volume").source("hadoop-logs-volume").target("/opt/hadoop/logs").build();
        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image("dsrw/hdfs:2.7.3-1")
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L, 3))
                        .mounts(Arrays.asList(mount))
                        .args("hdfs").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        List<PortConfig> portConfigs = new ArrayList<>();
        PortConfig port1 = PortConfig.builder().targetPort(8020).protocol("TCP").name("hdfs").build();
        PortConfig port2 = PortConfig.builder().targetPort(50090).protocol("TCP").name("hdfs-secondary").build();
        PortConfig port3 = PortConfig.builder().targetPort(50010).protocol("TCP").name("hdfs-datanode").build();
        PortConfig port4 = PortConfig.builder().targetPort(50020).protocol("TCP").name("hdfs-datanode-ipc").build();
        PortConfig port5 = PortConfig.builder().targetPort(50075).protocol("TCP").name("hdfs-datanode-http").build();
        PortConfig port6 = PortConfig.builder().targetPort(50070).protocol("TCP").name("hdfs-web").build();
        portConfigs.add(port1);
        portConfigs.add(port2);
        portConfigs.add(port3);
        portConfigs.add(port4);
        portConfigs.add(port5);
        portConfigs.add(port6);

        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target("network-name").build())
                .endpointSpec(EndpointSpec.builder().ports(portConfigs)
                        .build()).build();
        return spec;
    }
}