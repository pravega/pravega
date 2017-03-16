/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.client;

/**
 * Various types of stores.
 */
public enum StoreType {
    InMemory,
    Zookeeper,
    ECS,
    S3,
    HDFS
}
