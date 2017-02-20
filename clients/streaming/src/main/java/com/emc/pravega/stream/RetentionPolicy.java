/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class RetentionPolicy implements Serializable {
    private final long retentionTimeMillis;
}
