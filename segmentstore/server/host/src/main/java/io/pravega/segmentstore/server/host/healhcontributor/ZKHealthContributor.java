package io.pravega.segmentstore.server.host.healhcontributor;

import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.impl.AbstractHealthContributor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ZKHealthContributor  extends AbstractHealthContributor {
    private CuratorFramework zkClient;
    public ZKHealthContributor(String name, CuratorFramework zkClient) {
        super(name);
        this.zkClient = zkClient;
    }

    @Override
    public Status doHealthCheck(Health.HealthBuilder builder) {
        Status status = Status.DOWN;
        boolean running = (zkClient.getState() == CuratorFrameworkState.STARTED);
        if (running) {
            status = Status.NEW;
        }

        boolean ready = (zkClient.getZookeeperClient().isConnected());
        if (ready) {
            status = Status.UP;
        }

        Map<String, Object> details = new HashMap<String, Object>();
        details.put("zkclient-isconnected", zkClient.getZookeeperClient().isConnected());
        builder.details(details);
        return status;
    }

}