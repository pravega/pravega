/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.shared.controller.event.StreamEvent;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.UUID;

@Data
@EqualsAndHashCode(callSuper = false)
public class AbortEvent extends StreamEvent {
    private final int epoch;
    private final UUID txid;

    public AbortEvent(String scope, String stream, int epoch, UUID txid) {
        super(scope, stream);
        this.epoch = epoch;
        this.txid = txid;
    }
}
