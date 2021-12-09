/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.state.impl;

import io.pravega.client.state.Update;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;

import java.util.List;

import lombok.Data;

@Data
public class UpdateOrInit<StateT extends Revisioned> {
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
