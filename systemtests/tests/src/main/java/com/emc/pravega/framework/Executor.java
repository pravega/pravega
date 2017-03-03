/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.framework;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Executor {
    private String id;
    private String container;
    private String directory;
}
