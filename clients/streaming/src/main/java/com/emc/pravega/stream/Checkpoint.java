/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import java.io.Serializable;

public interface Checkpoint extends Serializable {

    String getName();
    
}
