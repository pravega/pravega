/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.state.impl;

import io.pravega.client.state.Update;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;

import java.util.List;

import lombok.Data;

@Data
class UpdateOrInit<StateT extends Revisioned> {
    private final List<? extends Update<StateT>> updates;
    private final InitialUpdate<StateT> init;

    UpdateOrInit(List<? extends Update<StateT>> updates) {
        this.updates = updates;
        this.init = null;
    }

    UpdateOrInit(InitialUpdate<StateT> init) {
        this.updates = null;
        this.init = init;
    }

    boolean isInit() {
        return updates == null;
    }
}
