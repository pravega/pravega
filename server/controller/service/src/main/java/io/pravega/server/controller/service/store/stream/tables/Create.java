/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.stream.tables;

import io.pravega.client.stream.StreamConfiguration;
import lombok.Data;

@Data
public class Create {
    private final long creationTime;
    private final StreamConfiguration configuration;
}
