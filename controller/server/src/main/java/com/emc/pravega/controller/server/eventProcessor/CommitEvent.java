/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.controller.requests.ControllerEvent;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.UUID;

@Data
@AllArgsConstructor
public class CommitEvent implements ControllerEvent {
    private final String scope;
    private final String stream;
    private final UUID txid;

    @Override
    public RequestType getType() {
        return RequestType.CommitEvent;
    }

    @Override
    public String getKey() {
        return String.format("%s/%s", scope, stream);
    }
}
