package io.pravega.shared.health.bindings.generated.api.factories;

import io.pravega.shared.health.bindings.generated.api.HealthApiService;
import io.pravega.shared.health.bindings.generated.api.impl.HealthApiServiceImpl;


public class HealthApiServiceFactory {
    private final static HealthApiService service = new HealthApiServiceImpl();

    public static HealthApiService getHealthApi() {
        return service;
    }
}
