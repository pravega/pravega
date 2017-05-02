/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.controller.service.store.task;

import com.google.common.base.Preconditions;
import lombok.Data;

/**
 * Resources managed by controller.
 * Currently there are two kinds of resources.
 * 1. Stream resource: scope/streamName
 * 2, Tx resource:     scope/streamName/txId
 */
@Data
public class Resource {
    private final String string;

    public Resource(final String... parts) {
        Preconditions.checkNotNull(parts);
        Preconditions.checkArgument(parts.length > 0);
        StringBuilder representation = new StringBuilder(parts[0]);
        for (int i = 1; i < parts.length; i++) {
            representation.append("/");
            representation.append(parts[i]);
        }
        string = representation.toString();
    }
}
