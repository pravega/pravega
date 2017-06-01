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
import com.spotify.docker.client.messages.swarm.*;
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
public class HdfsDockerService  extends DockerBasedService {

    private int instances = 1;
    private double cpu = 0.1 * Math.pow(10.0, 9.0);
    private long mem = 2048 * 1024 * 1024L;

    public HdfsDockerService(final String serviceName) {
        super(serviceName);
    }
    @Override
    public void stop() {
        try {
            com.spotify.docker.client.messages.swarm.Service.Criteria criteria = com.spotify.docker.client.messages.swarm.Service.Criteria.builder().serviceName(this.serviceName).build();
            List<com.spotify.docker.client.messages.swarm.Service> serviceList = docker.listServices(criteria);
            for(int i=0;i< serviceList.size();i++) {
                String serviceId = serviceList.get(i).id();
                docker.removeService(serviceId);
            }
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
            if(wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        }  catch (InterruptedException | DockerException | TimeoutException | ExecutionException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {
        Mount mount = Mount.builder().type("volume").source("hadoop-logs-volume").target("/opt/hadoop/logs").build();
        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image("dsrw/hdfs:2.7.3-1")
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L,  3))
                        .mounts(Arrays.asList(mount))
                        .args("hdfs").build())
                .resources(ResourceRequirements.builder()
                        .limits(Resources.builder()
                                .memoryBytes(mem).nanoCpus((long) cpu).build())
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

        ServiceSpec spec =  ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target("network-name").build())
                .endpointSpec(EndpointSpec.builder().ports(portConfigs)
                        .build()).build();
        return  spec;
    }

}
