package com.emc.pravega.cluster.zkutils.abstraction;

/**
 * Created by kandha on 8/18/16.
 */
public enum ConfigSyncManagerType{
    /**
     * Dummy implementation for stand alone behavior
     */
    DUMMY,
    /**
     * Zookeeper based implementation
     */
    ZK,
    /**
     * Vnest based implementation
     */
    VNEST
}
