/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import java.util.UUID;
import lombok.Data;

@Data
public class TxnSegments {

    private final StreamSegments steamSegments;
    private final UUID txnId;
    
}
