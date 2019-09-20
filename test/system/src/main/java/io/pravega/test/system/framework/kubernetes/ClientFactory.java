/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.kubernetes;

/**
 * Client factory.
 */
public enum ClientFactory {

    INSTANCE;

    private final K8sClient client;

    ClientFactory() {
        this.client = new K8sClient();
    }

    public K8sClient getK8sClient() {
        return client;
    }

}
