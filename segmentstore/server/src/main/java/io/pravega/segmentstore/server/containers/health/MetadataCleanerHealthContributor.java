package io.pravega.segmentstore.server.containers.health;

import com.google.common.util.concurrent.Service;
import io.pravega.shared.health.contributors.ServiceHealthContributor;

public class MetadataCleanerHealthContributor extends ServiceHealthContributor {

    public MetadataCleanerHealthContributor(String name, Service service) {
        super(name, service);
    }

}
