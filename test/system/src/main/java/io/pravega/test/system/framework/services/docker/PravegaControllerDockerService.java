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
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import static io.pravega.test.system.framework.Utils.ALTERNATIVE_CONTROLLER_PORT;
import static io.pravega.test.system.framework.Utils.ALTERNATIVE_REST_PORT;
import static io.pravega.test.system.framework.Utils.DOCKER_CONTROLLER_PORT;
import static io.pravega.test.system.framework.Utils.DOCKER_NETWORK;
import static io.pravega.test.system.framework.Utils.REST_PORT;

@Slf4j
public class PravegaControllerDockerService extends DockerBasedService {

    private final int instances = 1;
    private final double cpu = 0.5;
    private final double mem = 700.0;
    private final URI zkUri;
    private int controllerPort = DOCKER_CONTROLLER_PORT;
    private int restPort = REST_PORT;

    public PravegaControllerDockerService(final String serviceName, final URI zkUri) {
        super(serviceName);
        this.zkUri = zkUri;
        if (!serviceName.equals("controller")) {
            this.controllerPort = ALTERNATIVE_CONTROLLER_PORT;
            this.restPort = ALTERNATIVE_REST_PORT;
        }

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
        Mount mount = Mount.builder().type("Volume").source("controller-logs").target("/opt/pravega/logs").build();
        String zk = zkUri.getHost() + ":" + ZKSERVICE_ZKPORT;
        Map<String, String> stringBuilderMap = new HashMap<>();
        stringBuilderMap.put("controller.zk.connect.uri", zk);
        stringBuilderMap.put("controller.service.rpc.published.host.nameOrIp", serviceName);
        stringBuilderMap.put("controller.service.rpc.published.port", String.valueOf(controllerPort));
        stringBuilderMap.put("controller.service.rpc.listener.port", String.valueOf(controllerPort));
        stringBuilderMap.put("controller.service.rest.listener.port", String.valueOf(restPort));
        stringBuilderMap.put("log.level", "DEBUG");
        stringBuilderMap.put("curator-default-session-timeout", String.valueOf(10 * 1000));
        stringBuilderMap.put("controller.zk.connect.session.timeout.milliseconds", String.valueOf(30 * 1000));
        stringBuilderMap.put("controller.transaction.lease.count.max", String.valueOf(600 * 1000));
        stringBuilderMap.put("controller.retention.frequency.minutes", String.valueOf(2));
        StringBuilder systemPropertyBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : stringBuilderMap.entrySet()) {
            systemPropertyBuilder.append("-D").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }

        String controllerSystemProperties = systemPropertyBuilder.toString();
        String env1 = "PRAVEGA_CONTROLLER_OPTS=" + controllerSystemProperties;
        String env2 = "JAVA_OPTS=-Xmx512m";
        Map<String, String> labels = new HashMap<>();
        labels.put("com.docker.swarm.task.name", serviceName);

        final TaskSpec taskSpec = TaskSpec
                .builder()
                .networks(NetworkAttachmentConfig.builder().target(DOCKER_NETWORK).aliases(serviceName).build())
                .containerSpec(ContainerSpec.builder().image(IMAGE_PATH + IMAGE_PREFIX + PRAVEGA_IMAGE_NAME + PRAVEGA_VERSION)
                        .healthcheck(ContainerConfig.Healthcheck.builder().test(defaultHealthCheck(controllerPort)).build())
                        .mounts(Arrays.asList(mount))
                        .hostname(serviceName)
                        .labels(labels)
                        .env(Arrays.asList(env1, env2)).args("controller").build())
                .resources(ResourceRequirements.builder()
                        .reservations(Resources.builder()
                                .memoryBytes(setMemInBytes(mem)).nanoCpus(setNanoCpus(cpu)).build())
                        .build())
                .build();
        ServiceSpec spec = ServiceSpec.builder().name(serviceName).taskTemplate(taskSpec).mode(ServiceMode.withReplicas(instances))
                .endpointSpec(EndpointSpec.builder()
                        .ports(Arrays.asList(PortConfig.builder()
                                        .publishedPort(controllerPort).targetPort(controllerPort).publishMode(PortConfig.PortConfigPublishMode.HOST).build(),
                                PortConfig.builder().publishedPort(restPort).targetPort(restPort).publishMode(PortConfig.PortConfigPublishMode.HOST).build())).
                                build())
                .build();
        return spec;
    }
}
