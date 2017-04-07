/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.stream.StreamConfiguration;
import lombok.Data;

@Data
public class Create {
    private final long creationTime;
    private final StreamConfiguration configuration;
}
