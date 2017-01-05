/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Update;

import java.util.List;

import lombok.Data;

@Data
class UpdateOrInit<StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> {
    private final List<? extends UpdateT> updates;
    private final InitT init;
    private final Revision initRevision;

    UpdateOrInit(List<? extends UpdateT> updates) {
        this.updates = updates;
        this.init = null;
        this.initRevision = null;
    }

    UpdateOrInit(InitT init, Revision revision) {
        this.updates = null;
        this.init = init;
        this.initRevision = revision;
    }

    boolean isInit() {
        return updates == null;
    }
}
