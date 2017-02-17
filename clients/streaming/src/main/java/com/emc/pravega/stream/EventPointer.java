/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.EventPointerInternal;

import java.io.Serializable;

public interface EventPointer extends Serializable {

    /**
     * Used internally. Do not call.
     */
    EventPointerInternal asImpl();

}
