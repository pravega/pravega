/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import com.spotify.docker.client.messages.swarm.RestartPolicy;
import com.spotify.docker.client.messages.swarm.ServiceMode;
import com.spotify.docker.client.messages.swarm.ServiceSpec;
import com.spotify.docker.client.messages.swarm.TaskSpec;
import io.pravega.common.Exceptions;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

@Slf4j
public class BookkeeperDockerService extends DockerBasedService {

    private static final int BK_PORT = 3181;
    private final long instances = 3;
    private final double cpu = 0.1;
    private final double mem = 1024.0;
    private URI zkUri;

    public BookkeeperDockerService(String serviceName, final URI zkUri) {
        super(serviceName);
        this.zkUri = zkUri;
    }

    @Override
    public void stop() {
        try {
            Exceptions.handleInterrupted(() -> dockerClient.removeService(getID()));
        } catch (DockerException e) {
            log.error("Unable to remove service", e);
        }
    }

    @Override
    public void clean() {
    }

    @Override
    public void start(final boolean wait) {
        try {
            ServiceCreateResponse serviceCreateResponse = Exceptions.handleInterrupted(() -> dockerClient.createService(setServiceSpec()));
            if (wait) {
                Exceptions.handleInterrupted(() -> waitUntilServiceRunning().get(5, TimeUnit.MINUTES));
            }
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        } catch (Exception e) {
            log.error("Unable to create service", e);
        }
    }

    private ServiceSpec setServiceSpec() {

        Mount mount1 = Mount.builder().type("volume").source("index-volume").target("/bk/index")
                .build();
        Mount mount2 = Mount.builder().type("volume").source("logs-volume")
                .target("/opt/dl_all/distributedlog-service/logs/")
                .build();
        //TODO: add journal and ledger mounted volumes
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        List<String> stringList = new ArrayList<>();
        String env1 = "ZK_URL=" + zk;
        String env2 = "ZK=" + zk;
        String env3 = "bookiePort=" + String.valueOf(BK_PORT);
        String env4 = "DLOG_EXTRA_OPTS=-Xms512m";
        stringList.add(env1);
        stringList.add(env2);
        stringList.add(env3);
        stringList.add(env4);
        final TaskSpec taskSpec = TaskSpec
                .builder().restartPolicy(RestartPolicy.builder().maxAttempts(0).condition("none").build())
                .containerSpec(ContainerSpec.builder()
                        .hostname(serviceName)
                        .image(IMAGE_PATH + "/nautilus/bookkeeper:" + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L, 3))
                        .mounts(Arrays.asList(mount1, mount2))
                        .env(stringList).build())
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName)
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .endpointSpec(EndpointSpec.builder().ports(PortConfig.builder().publishedPort(BK_PORT).build()).build())
                .taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .build();
        return spec;
    }
}
