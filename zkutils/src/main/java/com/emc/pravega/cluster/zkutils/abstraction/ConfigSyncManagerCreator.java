package com.emc.pravega.cluster.zkutils.abstraction;

import com.emc.pravega.cluster.zkutils.dummy.DummyZK;
import com.emc.pravega.cluster.zkutils.vnest.Vnest;
import com.emc.pravega.cluster.zkutils.zkimplementation.ZookeeperClient;

;

public final class ConfigSyncManagerCreator {
    public ConfigSyncManager createManager(ConfigSyncManagerType type, String connectionString, int timeoutms,
                                           ConfigChangeListener listener) throws Exception {
        switch(type) {
            case DUMMY:
                return new DummyZK(connectionString, timeoutms, listener);
            case ZK:
                    return new ZookeeperClient(connectionString,timeoutms, listener);
            case VNEST:
                return new Vnest(connectionString,timeoutms, listener);
            default:
                throw new UnsupportedOperationException();

        }
    }
}
