/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.controller.eventProcessor.ControllerEvent;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.UUID;

@Data
@AllArgsConstructor
public class AbortEvent implements ControllerEvent {
    private final String scope;
    private final String stream;
    private final UUID txid;
}
