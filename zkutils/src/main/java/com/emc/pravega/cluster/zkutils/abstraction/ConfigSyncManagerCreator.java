package com.emc.pravega.cluster.zkutils.abstraction;

;import com.emc.pravega.cluster.zkutils.dummy.DummyZK;
import com.emc.pravega.cluster.zkutils.vnest.Vnest;
import com.emc.pravega.cluster.zkutils.zkimplementation.ZookeeperClient;

import java.io.IOException;

public final class ConfigSyncManagerCreator {
    public ConfigSyncManager createManager(ConfigSyncManagerType type, String connectionString, int timeout) throws IOException {
        switch(type) {
            case DUMMY:
                return new DummyZK(connectionString, timeout);
            case ZK:
                return new ZookeeperClient(connectionString,timeout);
            case VNEST:
                return new Vnest(connectionString,timeout);

        }
        return null;
    }
}
