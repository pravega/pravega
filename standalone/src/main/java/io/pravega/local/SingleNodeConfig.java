/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.local;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

public class SingleNodeConfig {
    //region config names
    public final static Property<Integer> ZK_PORT = Property.named("zkPort", 4000);
    public final static Property<Integer> SEGMENTSTORE_PORT = Property.named("segmentstorePort", 6000);
    public final static Property<Integer> CONTROLLER_PORT = Property.named("controllerPort", 9090);
    public final static Property<Integer> REST_SERVER_PORT = Property.named("restServerPort", 9091);
    public final static Property<String> CERT_FILE = Property.named("certFile", "");
    public final static Property<String> KEY_FILE = Property.named("keyFile", "");
    public final static Property<String> PASSWD_FILE = Property.named("passwdFile", "");
    public final static Property<String> USER_NAME = Property.named("userName", "");
    public final static Property<String> PASSWD = Property.named("passwd", "");
    public final static Property<Boolean> ENABLE_REST_SERVER = Property.named("enableRestServer", true);
    public final static Property<Boolean> ENABLE_TLS = Property.named("enableTls", false);
    public final static Property<Boolean> ENABLE_AUTH = Property.named("enableAuth", false);
    public final static String PROPERTY_FILE = "singlenode.configurationFile";
    public final static Property<String> KEYSTORE_JKS = Property.named("singlenode.keyStoreJKS", "");
    public final static Property<String> KEYSTORE_JKS_PASSWORD = Property.named("singlenode.keyStoreJKSPassword", "");

    private static final String COMPONENT_CODE = "singlenode";
    //end region

    //region members

    /**
     * The Zookeeper port for singlenode
     */
    @Getter
    private final int zkPort;

    /**
     * The SegmentStore port for singlenode
     */
    @Getter
    private final int segmentStorePort;

    /**
     * The controller port for singlenode
     */
    @Getter
    private final int controllerPort;

    /**
     * The REST server port for singlenode
     */
    @Getter
    private final int restServerPort;

    /**
     * The file containing the client certificate.
     */
    @Getter
    private String certFile;

    /**
     * File containing the certificate key for the server socket.
     */
    @Getter
    private String keyFile;

    /**
     * File containing the username passwd db for default auth handler.
     */
    @Getter
    private String passwdFile;

    /**
     * User for the authenticated interactions between segmentstore and controller.
     */
    @Getter
    private String userName;

    /**
     * Password for the user.
     */
    @Getter
    private String passwd;

    /**
     * JKS key store for secure access to controller REST API.
     */
    @Getter
    private final String keyStoreJKS;

    /**
     * Password file for JKS key store for secure access to controller REST API.
     */
    @Getter
    private final String keyStoreJKSPassword;

    /**
     * Flag to enable REST server.
     */
    @Getter
    private boolean enableRestServer;

    /**
     * Flag to enable TLS.
     */
    @Getter
    private boolean enableTls;

    /**
     * Flag to enable auth.
     */
    @Getter
    private boolean enableAuth;

    //end region

    //region constructor
    private SingleNodeConfig(TypedProperties properties) {
        this.zkPort = properties.getInt(ZK_PORT);
        this.segmentStorePort = properties.getInt(SEGMENTSTORE_PORT);
        this.controllerPort = properties.getInt(CONTROLLER_PORT);
        this.restServerPort = properties.getInt(REST_SERVER_PORT);
        this.certFile = properties.get(CERT_FILE);
        this.keyFile = properties.get(KEY_FILE);
        this.passwdFile = properties.get(PASSWD_FILE);
        this.userName = properties.get(USER_NAME);
        this.passwd = properties.get(PASSWD);
        this.enableRestServer = properties.getBoolean(ENABLE_REST_SERVER);
        this.enableTls = properties.getBoolean(ENABLE_TLS);
        this.enableAuth = properties.getBoolean(ENABLE_AUTH);
        keyStoreJKS = properties.get(KEYSTORE_JKS);
        keyStoreJKSPassword = properties.get(KEYSTORE_JKS_PASSWORD);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<SingleNodeConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, SingleNodeConfig::new);
    }

    //end region
}
