/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import lombok.Data;

import java.util.UUID;

@Data
public class ActiveTxRecordWithStream {
    private final String scope;
    private final String stream;
    private final UUID txid;
    private final ActiveTxRecord txRecord;
}
