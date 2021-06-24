package io.pravega.segmentstore.server.host.rest.generated.api.factories;

import io.pravega.segmentstore.server.host.rest.generated.api.HealthApiService;
import io.pravega.segmentstore.server.host.rest.generated.api.impl.HealthApiServiceImpl;


public class HealthApiServiceFactory {
    private final static HealthApiService service = new HealthApiServiceImpl();

    public static HealthApiService getHealthApi() {
        return service;
    }
}
