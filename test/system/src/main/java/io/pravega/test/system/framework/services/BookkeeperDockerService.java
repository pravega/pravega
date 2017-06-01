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
import com.spotify.docker.client.messages.mount.BindOptions;
import com.spotify.docker.client.messages.mount.Mount;
import com.spotify.docker.client.messages.mount.VolumeOptions;

import static io.pravega.test.system.framework.services.MarathonBasedService.IMAGE_PATH;
import static io.pravega.test.system.framework.services.MarathonBasedService.PRAVEGA_VERSION;

import com.spotify.docker.client.messages.swarm.*;
import com.spotify.docker.client.messages.swarm.Service;
import lombok.extern.slf4j.Slf4j;
import java.net.URI;
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
public class BookkeeperDockerService extends DockerBasedService {

    private static final int BK_PORT = 3181;
    //private final URI zkUri;
    //private String serviceId;
    private int instances = 3;
    private double cpu = 0.1 * Math.pow(10.0, 9.0);
    private long mem = 1024 * 1024 * 1024L;

    public BookkeeperDockerService(String serviceName) {
        super(serviceName);
        //this.zkUri = zkUri;
    }

    @Override
    public void stop() {
        try {
            Service.Criteria criteria = Service.Criteria.builder().serviceName(this.serviceName).build();
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
            if (wait) {
                waitUntilServiceRunning().get(5, TimeUnit.MINUTES);
            }
            assertThat(serviceCreateResponse.id(), is(notNullValue()));
        }  catch (InterruptedException | DockerException | TimeoutException | ExecutionException e) {
            log.error("unable to create service {}", e);
        }
    }

    private ServiceSpec setServiceSpec() {
        Mount mount1 = Mount.builder().type("volume").source("journal-volume").target("/bk/journal")
               .build();
        Mount mount2 = Mount.builder().type("volume").source("index-volume").target("/bk/index")
               .build();
        Mount mount3 = Mount.builder().type("volume").source("ledgers-volume").target("/bk/ledger")
                .build();
        Mount mount4 = Mount.builder().type("volume").source("logs-volume")
                .target("/opt/dl_all/distributedlog-service/logs/")
                .build();

        String zk = "zookeeper" + ":" + ZKSERVICE_ZKPORT;
        List<String> stringList = new ArrayList<>();
        String env1 = "ZK_URL="+zk;
        String env2 = "ZK="+zk;
        String env3 = "bookiePort="+String.valueOf(BK_PORT);
        String env4 =  "DLOG_EXTRA_OPTS=-Xms512m";
        stringList.add(env1);
        stringList.add(env2);
        stringList.add(env3);
        stringList.add(env4);
        final TaskSpec taskSpec = TaskSpec
                .builder().restartPolicy(RestartPolicy.builder().maxAttempts(0).condition("none").build())
                .containerSpec(ContainerSpec.builder()
                        .image("asdrepo.isus.emc.com:8103" + "/nautilus/bookkeeper:0.1.0-1415.b5f03f5")
                        .command("/bin/sh","-c",
                        "/opt/bk_all/entrypoint.sh")
                        .healthcheck(ContainerConfig.Healthcheck.create(null, 1000000000L, 1000000000L, 3))
                        .mounts(Arrays.asList(mount1, mount2, mount3, mount4))
                        .env(stringList).build())
                .resources(ResourceRequirements.builder()
                        .limits(Resources.builder()
                                .memoryBytes(mem).nanoCpus((long) cpu).build())
                        .build())
                .build();
        ServiceSpec spec =  ServiceSpec.builder().name(serviceName)
                .networks(NetworkAttachmentConfig.builder().target("network-name").build())
                .taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder().addPort(PortConfig.builder().
                        targetPort(BK_PORT).protocol("TCP").build())
                        .build()).build();
        return spec;
    }
    public static void main(String[] args) {
        ZookeeperDockerService zookeeperDockerService = new ZookeeperDockerService("zookeeper");
        if(!zookeeperDockerService.isRunning()) {
            zookeeperDockerService.start(true);
        }
        List<URI> zkUris = zookeeperDockerService.getServiceDetails();
        BookkeeperDockerService bookkeeperDockerService = new BookkeeperDockerService("bookkeeper");
        log.debug("zk uri details {}", zkUris.get(0).toString());
        if(!bookkeeperDockerService.isRunning()) {
            bookkeeperDockerService.start(true);
        }
    }
}
