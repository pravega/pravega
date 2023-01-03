/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.local;

import io.pravega.common.security.TLSProtocolVersion;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

public class SingleNodeConfig {
    //region config names
    public final static String PROPERTY_FILE = "singlenode.configurationFile";
    public final static String PROPERTY_FILE_DEFAULT_PATH = "./config/standalone-config.properties";

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
    public final static Property<String> TLS_PROTOCOL_VERSION = Property.named("security.tls.protocolVersion", "TLSv1.2,TLSv1.3");
    public final static Property<String> KEY_FILE = Property.named("security.tls.privateKey.location", "", "keyFile");
    public final static Property<String> CERT_FILE = Property.named("security.tls.certificate.location", "", "certFile");
    public final static Property<String> KEYSTORE_JKS = Property.named("security.tls.keyStore.location", "", "keyStoreJKS");
    public final static Property<String> KEYSTORE_JKS_PASSWORD_FILE = Property.named("security.tls.keyStore.pwd.location", "", "keyStoreJKSPasswordFile");
    public final static Property<String> TRUSTSTORE_JKS = Property.named("security.tls.trustStore.location", "", "trustStoreJKS");

    // Metrics configurations
    public final static Property<Boolean> ENABLE_METRICS = Property.named("metrics.enable", false);
    public final static Property<Boolean> ENABLE_INFLUX_REPORTER = Property.named("metrics.influx.enable", false);
    public final static Property<Boolean> ENABLE_PROMETHEUS = Property.named("metrics.prometheus.enable", false);
    public final static Property<Integer> METRICS_REPORT_INTERVAL = Property.named("metrics.reporting.interval", 60);

    // Pravega Admin Gateway configuration
    public static final Property<Boolean> ENABLE_ADMIN_GATEWAY = Property.named("admin.gateway.enable", true);
    public static final Property<Integer> ADMIN_GATEWAY_PORT = Property.named("admin.gateway.port", 9999);

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
     *
     */
    @Getter
    private String[] tlsProtocolVersion;

    /**
     * Flag to enable auth.
     */
    @Getter
    private boolean enableAuth;


    /**
     * Flag to enable metrics.
     */
    @Getter
    private boolean enableMetrics;

    /**
     * Flag to enable InfluxDB reporting.
     */
    @Getter
    private boolean enableInfluxDB;

    /**
     * Flag to enable Prometheus.
     */
    @Getter
    private boolean enablePrometheus;

    /**
     * Flag to control the rate of reporting.
     */
    @Getter
    private int metricsReportInterval;

    /**
     * Defines whether to enable the Pravega Admin Gateway.
     */
    @Getter
    private final boolean enableAdminGateway;

    /**
     * Port to bing the Pravega Admin Gateway.
     */
    @Getter
    private final int adminGatewayPort;

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
        this.tlsProtocolVersion = new TLSProtocolVersion(properties.get(TLS_PROTOCOL_VERSION)).getProtocols();
        this.enableAuth = properties.getBoolean(ENABLE_AUTH);
        this.keyStoreJKS = properties.get(KEYSTORE_JKS);
        this.keyStoreJKSPasswordFile = properties.get(KEYSTORE_JKS_PASSWORD_FILE);
        this.trustStoreJKS = properties.get(TRUSTSTORE_JKS);
        this.enableSegmentStoreTlsReload = properties.getBoolean(ENABLE_TLS_RELOAD);
        this.enableMetrics = properties.getBoolean(ENABLE_METRICS);
        this.enableInfluxDB = properties.getBoolean(ENABLE_INFLUX_REPORTER);
        this.enablePrometheus = properties.getBoolean(ENABLE_PROMETHEUS);
        this.metricsReportInterval = properties.getInt(METRICS_REPORT_INTERVAL);
        this.enableAdminGateway = properties.getBoolean(ENABLE_ADMIN_GATEWAY);
        this.adminGatewayPort = properties.getInt(ADMIN_GATEWAY_PORT);
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
