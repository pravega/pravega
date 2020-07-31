/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
    public final static String PROPERTY_FILE = "singlenode.configurationFile";

    public final static Property<Integer> ZK_PORT = Property.named("zk.port", 4000, "zkPort");
    public final static Property<Integer> SEGMENTSTORE_PORT = Property.named("segmentStore.port", 6000, "segmentstorePort");
    public final static Property<Boolean> ENABLE_TLS_RELOAD = Property.named("segmentStore.tls.certificate.autoReload.enable", false, "segmentstoreEnableTlsReload");

    public final static Property<Integer> CONTROLLER_PORT = Property.named("controller.rpc.port", 9090, "controllerPort");
    public final static Property<Integer> REST_SERVER_PORT = Property.named("controller.rest.port", 9091, "restServerPort");
    public final static Property<Boolean> ENABLE_REST_SERVER = Property.named("controller.rest.enable", true, "enableRestServer");

    public final static Property<Boolean> ENABLE_AUTH = Property.named("security.auth.enable", false, "enableAuth");
    public final static Property<String> USER_NAME = Property.named("security.auth.credentials.username", "", "userName");
    public final static Property<String> PASSWD = Property.named("security.auth.credentials.pwd", "", "passwd");
    public final static Property<String> PASSWD_FILE = Property.named("security.auth.pwdAuthHandler.accountsDb.location", "", "passwdFile");

    // TLS-related configurations
    public final static Property<Boolean> ENABLE_TLS = Property.named("security.tls.enable", false, "enableTls");
    public final static Property<String> KEY_FILE = Property.named("security.tls.privateKey.location", "", "keyFile");
        public final static Property<String> CERT_FILE = Property.named("security.tls.certificate.location", "", "certFile");
    public final static Property<String> KEYSTORE_JKS = Property.named("security.tls.keyStore.location", "", "keyStoreJKS");
    public final static Property<String> KEYSTORE_JKS_PASSWORD_FILE = Property.named("security.tls.keyStore.pwd.location", "", "keyStoreJKSPasswordFile");
    public final static Property<String> TRUSTSTORE_JKS = Property.named("security.tls.trustStore.location", "", "trustStoreJKS");

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

    @Getter
    private boolean enableSegmentStoreTlsReload;

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
    private final String keyStoreJKSPasswordFile;

    /**
     * JKS trust store for secure access to controller REST API.
     */
    @Getter
    private final String trustStoreJKS;

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
        this.keyStoreJKS = properties.get(KEYSTORE_JKS);
        this.keyStoreJKSPasswordFile = properties.get(KEYSTORE_JKS_PASSWORD_FILE);
        this.trustStoreJKS = properties.get(TRUSTSTORE_JKS);
        this.enableSegmentStoreTlsReload = properties.getBoolean(ENABLE_TLS_RELOAD);
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
