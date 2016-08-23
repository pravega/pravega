package com.emc.pravega.controller.store.host;

import java.util.List;

public interface HostControllerStore {
    List<Host> getHosts();

    List<Integer> getContainersForHost(Host host);

    Host getHostForContainer(int containerId);
}
