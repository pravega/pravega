/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.test.system.framework.services.docker;

import com.spotify.docker.client.messages.ContainerConfig;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static io.pravega.test.system.framework.Utils.DOCKER_NETWORK;

@Slf4j
public class HDFSDockerService extends DockerBasedService {

    private static final int HDFS_PORT = 8020;
    private final int instances = 1;
    private final double cpu = 0.5;
    private final double mem = 2048.0;
    private final String hdfsimage = "pravega/hdfs:2.7.7";

    public HDFSDockerService(final String serviceName) {
        super(serviceName);
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void clean() {
    }

    @Override
    public void start(final boolean wait) {
        start(wait, setServiceSpec());
    }

    private ServiceSpec setServiceSpec() {
        Map<String, String> labels = new HashMap<>();
        labels.put("com.docker.swarm.task.name", serviceName);
        Mount mount = Mount.builder().type("volume").source("hadoop-logs").target("/opt/hadoop/logs").build();
        String env1 = "SSH_PORT=2222";
        String env2 = "HDFS_HOST=" + serviceName;

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .containerSpec(ContainerSpec.builder().image(hdfsimage).env(Arrays.asList(env1, env2))
                        .healthcheck(ContainerConfig.Healthcheck.builder().test(customHealthCheck("ss -l | grep " + HDFS_PORT + " || exit 1")).build())
                        .mounts(mount)
                        .labels(labels)
                        .hostname(serviceName)
                        .build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        List<PortConfig> portConfigs = new ArrayList<>();
        PortConfig port1 = PortConfig.builder().publishedPort(8020).targetPort(8020).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs").build();
        PortConfig port2 = PortConfig.builder().publishedPort(50090).targetPort(50090).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs-secondary").build();
        PortConfig port3 = PortConfig.builder().publishedPort(50010).targetPort(50010).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs-datanode").build();
        PortConfig port4 = PortConfig.builder().publishedPort(50020).targetPort(50020).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs-datanode-ipc").build();
        PortConfig port5 = PortConfig.builder().publishedPort(50075).targetPort(50075).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs-datanode-http").build();
        PortConfig port6 = PortConfig.builder().publishedPort(50070).targetPort(50070).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs-web").build();
        PortConfig port7 = PortConfig.builder().publishedPort(2222).targetPort(2222).publishMode(PortConfig.PortConfigPublishMode.HOST).name("hdfs-ssh").build();
        portConfigs.add(port1);
        portConfigs.add(port2);
        portConfigs.add(port3);
        portConfigs.add(port4);
        portConfigs.add(port5);
        portConfigs.add(port6);
        portConfigs.add(port7);

        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .endpointSpec(EndpointSpec.builder().ports(portConfigs)
                        .build()).build();
        return spec;
    }
}
