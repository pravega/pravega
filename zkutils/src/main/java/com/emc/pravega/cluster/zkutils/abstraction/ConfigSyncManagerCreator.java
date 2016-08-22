package com.emc.pravega.cluster.zkutils.abstraction;

;import com.emc.pravega.cluster.zkutils.dummy.DummyZK;
import com.emc.pravega.cluster.zkutils.vnest.Vnest;
import com.emc.pravega.cluster.zkutils.zkimplementation.ZookeeperClient;

import java.io.IOException;

public final class ConfigSyncManagerCreator {
    public ConfigSyncManager createManager(ConfigSyncManagerType type, String connectionString, int timeout,
                                           ConfigChangeListener listener) throws IOException {
        switch(type) {
            case DUMMY:
                return new DummyZK(connectionString, timeout, listener);
            case ZK:
                try {
                    return new ZookeeperClient(connectionString,timeout, listener);
                } catch (Exception e) {
                    return null;
                }
            case VNEST:
                return new Vnest(connectionString,timeout, listener);

        }
        return null;
    }
}
