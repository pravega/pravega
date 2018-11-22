/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework.services.kubernetes;

import io.pravega.test.system.framework.kubernetes.K8Client;
import io.pravega.test.system.framework.services.Service;

public abstract class AbstractService implements Service {

    static final int DEFAULT_CONTROLLER_COUNT = 1;
    static final int DEFAULT_SEGMENTSTORE_COUNT = 1;
    static final int DEFAULT_BOOKIE_COUNT = 3;
    static final String DEFAULT_IMAGE_PULL_POLICY = "IfNotPresent";
    static final int MIN_READY_SECONDS = 10; // minimum duration the operator is up and running to be considered ready.
    static final int ZKPORT = 2181;
    static final String NAMESPACE = "default";

    static final String PRAVEGA_OPERATOR = "pravega-operator";
    static final String CUSTOM_RESOURCE_GROUP_PRAVEGA = "pravega.pravega.io";
    static final String CUSTOM_RESOURCE_VERSION_PRAVEGA = "v1alpha1";
    static final String CUSTOM_RESOURCE_PLURAL_PRAVEGA = "pravegaclusters";
    static final String CUSTOM_RESOURCE_KIND_PRAVEGA = "PravegaCluster";
    static final String PRAVEGA_CONTROLLER_LABEL = "pravega-controller";
    static final String PRAVEGA_ID = "pravega";

    final K8Client k8Client;
    private final String id;

    AbstractService(final String id) {
        this.k8Client = new K8Client();
        this.id = id;
    }

    @Override
    public String getID() {
        return id;
    }
}
