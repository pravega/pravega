/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.cluster;

/**
 * Various kinds of cluster.
 */
public enum ClusterType {
    Host("hosts"),
    Controller("controllers");

    private final String text;

    ClusterType(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return this.text;
    }
}
