/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Update;

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
