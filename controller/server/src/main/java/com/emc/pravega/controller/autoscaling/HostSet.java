/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.autoscaling;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.store.host.HostChangeListener;
import com.emc.pravega.controller.store.host.HostControllerStore;

import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

/**
 * Observable set of hosts. Any changes to host cluster results in a change in this object. It notifies
 * all its observers of the said change.
 * Changes include addHost and removeHost.
 */
public class HostSet extends Observable implements HostChangeListener {
    private final Set<Host> hosts;

    public HostSet(final HostControllerStore hostStore) {
        hosts = new HashSet<>();
        hosts.addAll(hostStore.getAllHosts());
        hostStore.registerListener(this);
    }

    @Override
    public void addHost(final Host host) {
        hosts.add(host);
        setChanged();
        notifyObservers(host);
    }

    @Override
    public void removeHost(final Host host) {
        // TODO:
    }

    public Set<Host> getHosts() {
        return hosts;
    }

}
