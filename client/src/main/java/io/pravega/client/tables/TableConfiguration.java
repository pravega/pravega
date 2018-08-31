/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;


import io.pravega.client.stream.ScalingPolicy;
import java.io.Serializable;
import lombok.Builder;
import lombok.Data;

/**
 * The configuration of a Table.
 */
@Data
@Builder
public class TableConfiguration implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The Scope of the Table.
     *
     */
    private final String scope;

    /**
     * The name of the Table.
     *
     */
    private final String tableName;

    /**
     * The Table's Scaling Policy..
     *
     */
    private final ScalingPolicy scalingPolicy;

    public static final class TableConfigurationBuilder {
        private ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    }
}