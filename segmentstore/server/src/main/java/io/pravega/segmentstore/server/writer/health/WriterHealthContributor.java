package io.pravega.segmentstore.server.writer.health;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import io.pravega.shared.health.contributors.ServiceHealthContributor;

public class WriterHealthContributor extends ServiceHealthContributor {

    public WriterHealthContributor(String name, Service service) {
        super(name, service);
    }

}
