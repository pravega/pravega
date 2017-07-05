/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.storage.impl.extendeds3.AclSize;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class represents the ACL to object key mapping for S3 wrapper for integration tests.
 */
public class AclMap {
    private ConcurrentMap<String, AclSize> aclMap = new ConcurrentHashMap<>();

    public void addNewObject(String key, AclSize aclSize) {
        aclMap.putIfAbsent(key, aclSize);
    }

    public void updateObject(String key, AclSize aclSize) {
        aclMap.put(key, aclSize);
    }

    public AclSize getAclSizeForObject(String key) {
        return aclMap.get(key);
    }

    public void deleteObject(String key) {
        aclMap.remove(key);
    }

    public boolean containsKey(String key) {
        return aclMap.containsKey(key);
    }
}
