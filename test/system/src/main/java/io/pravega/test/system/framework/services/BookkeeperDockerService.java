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
import static io.pravega.test.system.framework.services.MarathonBasedService.IMAGE_PATH;
import static io.pravega.test.system.framework.services.MarathonBasedService.PRAVEGA_VERSION;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class BookkeeperDockerService extends DockerBasedService {

        private static final int BK_PORT = 3181;
        private final URI zkUri;
        private String serviceId;
        private int instances = 3;
        private double cpu = 0.1 * Math.pow(10.0, 9.0);
        private long mem = 1024 * 1024 * 1024L;


    public BookkeeperDockerService(String serviceName, final URI zkUri) {

        super(serviceName);
        this.zkUri = zkUri;

    }

    @Override
    public void stop() {
        try {
            docker.removeService(this.serviceId);
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
            serviceId = serviceCreateResponse.id();
            assertThat(serviceId, is(notNullValue()));
        }  catch (InterruptedException | DockerException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {

        Mount mount1 = Mount.builder().type("Volume").source("/mnt/journal").target("/bk/journal")
                .volumeOptions(VolumeOptions.builder().build()).readOnly(false).build();
        Mount mount2 = Mount.builder().type("Volume").source("/mnt/index").target("/bk/index")
                .volumeOptions(VolumeOptions.builder().build()).readOnly(false).build();
        Mount mount3 = Mount.builder().type("Volume").source("/mnt/ledgers").target("/bk/ledger")
                .volumeOptions(VolumeOptions.builder().build()).readOnly(false).build();
        Mount mount4 = Mount.builder().type("Volume").source("/mnt/logs")
                .target("/opt/dl_all/distributedlog-service/logs/").volumeOptions(VolumeOptions.builder().build()).readOnly(false).build();

        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        List<String> stringList = new ArrayList<>();
        String env1 = "ZK_URL="+zk;
        String env2 = "ZK="+zk;
        String env3 = "bookiePort"+String.valueOf(BK_PORT);
        String env4 =  "DLOG_EXTRA_OPTS=-Xms512m";
        stringList.add(env1);
        stringList.add(env2);
        stringList.add(env3);
        stringList.add(env4);

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + "/nautilus/bookkeeper:" + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 30L, 3L, 3))
                        .mounts(Arrays.asList(mount1, mount2, mount3, mount4))
                        .env(stringList).build())
                .resources(ResourceRequirements.builder()
                        .limits(Resources.builder()
                                .memoryBytes(mem).nanoCpus((long) cpu).build())
                        .build())
                .build();

        ServiceSpec spec =  ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder().addPort(PortConfig.builder().
                        publishedPort(BK_PORT).targetPort(BK_PORT).protocol("TCP").build())
                        .build()).build();
        return spec;
    }
}
