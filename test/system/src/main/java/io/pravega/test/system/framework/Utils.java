/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import io.pravega.test.system.framework.services.Service;
import io.pravega.test.system.framework.services.docker.BookkeeperDockerService;
import io.pravega.test.system.framework.services.docker.HDFSDockerService;
import io.pravega.test.system.framework.services.docker.PravegaControllerDockerService;
import io.pravega.test.system.framework.services.docker.PravegaSegmentStoreDockerService;
import io.pravega.test.system.framework.services.docker.ZookeeperDockerService;
import io.pravega.test.system.framework.services.kubernetes.ZookeeperService;
import io.pravega.test.system.framework.services.marathon.BookkeeperService;
import io.pravega.test.system.framework.services.marathon.PravegaControllerService;
import io.pravega.test.system.framework.services.marathon.PravegaSegmentStoreService;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;

/**
 * Utility methods used inside the TestFramework.
 */
@Slf4j
public class Utils {

    public static final int DOCKER_CONTROLLER_PORT = 9090;
    public static final int MARATHON_CONTROLLER_PORT = 9092;
    public static final int REST_PORT = 9091;
    public static final String DOCKER_NETWORK = "docker-network";
    public static final boolean DOCKER_BASED = Utils.isDockerExecEnabled();
    public static final int ALTERNATIVE_CONTROLLER_PORT = 9093;
    public static final int ALTERNATIVE_REST_PORT = 9094;
    private static final TestExecutorFactory.TestExecutorType EXECUTOR_TYPE = TestExecutorFactory.getTestExecutionType();

    /**
     * Get Configuration from environment or system property.
     * @param key Configuration key
     * @param defaultValue default value incase the property/env is not set
     * @return the configuration value.
     */
    public static String getConfig(final String key, final String defaultValue) {
        return System.getenv().getOrDefault(key, System.getProperty(key, defaultValue));
    }

    public static Service createZookeeperService() {
        switch (EXECUTOR_TYPE) {
            case REMOTE_SEQUENTIAL:
                return new io.pravega.test.system.framework.services.marathon.ZookeeperService("zookeeper");
            case DOCKER:
                return new ZookeeperDockerService("zookeeper");
            case K8:
            default:
                return new ZookeeperService();

        }
    }

    public static Service createBookkeeperService(final URI zkUri) {
        String serviceId = "bookkeeper";
        switch (EXECUTOR_TYPE) {
            case REMOTE_SEQUENTIAL:
                return new BookkeeperService(serviceId, zkUri);
            case DOCKER:
                return new BookkeeperDockerService(serviceId, zkUri);
            case K8:
            default:
                return new io.pravega.test.system.framework.services.kubernetes.BookkeeperService(serviceId, zkUri);
        }
    }

    public static Service createPravegaControllerService(final URI zkUri, String serviceName) {
        switch (EXECUTOR_TYPE) {
            case REMOTE_SEQUENTIAL:
                return new PravegaControllerService(serviceName, zkUri);
            case DOCKER:
                return new PravegaControllerDockerService(serviceName, zkUri);
            case K8:
            default:
                return new io.pravega.test.system.framework.services.kubernetes.PravegaControllerService(serviceName, zkUri);
        }

    }

    public static Service createPravegaControllerService(final URI zkUri) {
        return createPravegaControllerService(zkUri, "controller");
    }

    public static Service createPravegaSegmentStoreService(final URI zkUri, final URI contUri) {
        URI hdfsUri = null;
        if (DOCKER_BASED) {
            Service hdfsService = new HDFSDockerService("hdfs");
            if (!hdfsService.isRunning()) {
                hdfsService.start(true);
            }
            hdfsUri = hdfsService.getServiceDetails().get(0);
        }

        String serviceId = "segmentstore";
        switch (EXECUTOR_TYPE) {
            case REMOTE_SEQUENTIAL:
                return new PravegaSegmentStoreService(serviceId, zkUri, contUri);
            case DOCKER:
                return  new PravegaSegmentStoreDockerService(serviceId, zkUri, hdfsUri, contUri);
            case K8:
            default:
                return new io.pravega.test.system.framework.services.kubernetes.PravegaSegmentStoreService(serviceId, zkUri);
        }
    }

    /**
     * Helper method to check if skipServiceInstallation flag is set.
     * This flag indicates if the system test framework should reuse services already deployed on the cluster.
     * if set to
     *  true: Already deployed services are used for running tests.
     *  false: Services are deployed on the cluster before running tests.
     *
     * Default value is false
     * @return
     */
    public static boolean isSkipServiceInstallationEnabled() {
        String config = getConfig("skipServiceInstallation", "false");
        return config.trim().equalsIgnoreCase("true") ? true : false;
    }

    public static boolean isDockerExecEnabled() {
        String dockerConfig = getConfig("execType", "LOCAL");
        return dockerConfig.trim().equalsIgnoreCase("docker") ?  true : false;

    }

    public static boolean isAwsExecution() {
        String dockerConfig = getConfig("awsExec", "false");
        return dockerConfig.trim().equalsIgnoreCase("true") ?  true : false;

    }
}
