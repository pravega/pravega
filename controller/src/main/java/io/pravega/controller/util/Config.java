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
package io.pravega.controller.util;

import com.google.common.base.Strings;
import io.pravega.common.security.TLSProtocolVersion;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import io.pravega.controller.server.rpc.grpc.impl.GRPCServerConfigImpl;
import io.pravega.shared.metrics.MetricsConfig;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Utility class to supply Controller Configuration.
 *
 * The configuration values are retrieved using the following order, with every one overriding previously loaded values:
 * 1. The configuration file. By default a 'controller.config.properties' file is sought in the classpath; this can be
 * overridden by setting the 'conf.file' system property to point to another one.
 * 2. Environment Variables ({@link System#getenv()}).
 * 3. System Properties ({@link System#getProperties()}.
 * 4. All currently loaded values will be resolved against themselves.
 * 5. Anything which is not supplied via the methods above will be defaulted to the values defined in this class.
 *
 * Configuration values can be resolved against themselves by referencing them using a special syntax. Any value
 * of the form '${CFG}' will lookup the already loaded config value with key 'CFG' and, if defined and non-empty, it will
 * use that config value as the final value (if not defined or empty, it will not be included in the final result and the
 * default value (step 5) will be used). Chained resolution is not supported (i.e., CFG1=${CFG2};CFG2=${CFG3} will not
 * set CFG1 to the value of CFG3).
 */
@Slf4j
public final class Config {

    //region Property Definitions
    public static final String NULL_VALUE = "{null}";
    public static final Property<Integer> PROPERTY_CONTAINER_COUNT =
            Property.named("container.count", 4, "containerCount");

    public static final Property<Boolean> PROPERTY_HOST_MONITORING_ENABLED = Property.named(
            "hostMonitor.enable", true, "hostMonitorEnabled");

    public static final Property<Integer> PROPERTY_MIN_REBALANCE_INTERVAL_SECONDS = Property.named(
            "rebalance.interval.seconds.min", 10, "minRebalanceIntervalSeconds");

    public static final Property<Boolean> PROPERTY_REPLY_WITH_STACK_TRACE_ON_ERROR = Property.named(
            "request.replyWithStackTraceOnError.enable", false, "replyWithStackTraceOnError");

    public static final Property<Boolean> PROPERTY_REQUEST_TRACING_ENABLED = Property.named(
            "request.tracing.enable", true, "requestTracingEnabled");

    public static final Property<Boolean> PROPERTY_DUMP_STACK_ON_SHUTDOWN =
            Property.named("dumpStackOnShutdown.enable", false, "dumpStackOnShutdown");

    public static final Property<Boolean> PROPERTY_USE_PRAVEGA_TABLES = Property.named(
            "pravegaTables.enable", true, "usePravegaTables");

    public static final Property<Integer> PROPERTY_SERVICE_PORT = Property.named(
            "service.rpc.listener.port", 9090, "service.port");

    public static final Property<Integer> PROPERTY_TASK_POOL_SIZE = Property.named(
            "service.asyncTaskPool.size", 80, "service.asyncTaskPoolSize");

    public static final Property<String> PROPERTY_SERVICE_HOST_IP = Property.named(
            "segmentstore.connect.host.nameOrIp", "localhost", "service.hostIp");

    public static final Property<Integer> PROPERTY_SERVICE_HOST_PORT = Property.named(
            "segmentstore.connect.port", 12345, "service.hostPort");

    public static final Property<String> PROPERTY_RPC_HOST = Property.named(
            "service.rpc.published.host.nameOrIp", NULL_VALUE, "service.publishedRPCHost");

    public static final Property<Integer> PROPERTY_RPC_PORT = Property.named(
            "service.rpc.published.port", 9090, "service.publishedRPCPort");

    public static final Property<String> PROPERTY_CLUSTER_NAME = Property.named(
            "cluster.name", "pravega-cluster", "service.cluster");

    public static final Property<String> PROPERTY_REST_IP = Property.named(
            "service.rest.listener.host.ip", "0.0.0.0", "service.restIp");

    public static final Property<Integer> PROPERTY_REST_PORT = Property.named(
            "service.rest.listener.port", 9091, "service.restPort");

    public static final Property<String> PROPERTY_REST_KEYSTORE_FILE_PATH = Property.named(
            "security.tls.server.keyStore.location", "", "rest.tlsKeyStoreFile");

    public static final Property<String> PROPERTY_REST_KEYSTORE_PASSWORD_FILE_PATH = Property.named(
            "security.tls.server.keyStore.pwd.location", "", "rest.tlsKeyStorePasswordFile");

    public static final Property<Boolean> PROPERTY_AUTH_ENABLED = Property.named(
            "security.auth.enable", false, "auth.enabled");

    public static final Property<String> PROPERTY_PWD_AUTH_HANDLER_ACCOUNTS_STORE = Property.named(
            "security.pwdAuthHandler.accountsDb.location", "", "auth.userPasswordFile");

    public static final Property<String> PROPERTY_TOKEN_SIGNING_KEY = Property.named(
            "security.auth.delegationToken.signingKey.basis", "", "auth.tokenSigningKey");

    public static final Property<Integer> PROPERTY_ACCESS_TOKEN_TTL_SECONDS = Property.named(
            "security.auth.delegationToken.ttl.seconds", 600, "auth.accessTokenTtlSeconds");

    public static final Property<Boolean> PROPERTY_WRITES_TO_RGSTREAMS_WITH_READ_PERMISSIONS = Property.named(
            "security.auth.readerGroupStreams.writesWithReadPermissions.enable", true);

    public static final Property<Boolean> PROPERTY_TLS_ENABLED = Property.named(
            "security.tls.enable", false, "auth.tlsEnabled");

    public static final Property<String> PROPERTY_TLS_PROTOCOL_VERSION = Property.named(
            "security.tls.protocolVersion", "TLSv1.2,TLSv1.3");

    public static final Property<String> PROPERTY_TLS_CERT_FILE = Property.named(
            "security.tls.server.certificate.location", "", "auth.tlsCertFile");

    public static final Property<String> PROPERTY_TLS_TRUST_STORE = Property.named(
            "security.tls.trustStore.location", "", "auth.tlsTrustStore");

    public static final Property<String> PROPERTY_TLS_KEY_FILE = Property.named(
            "security.tls.server.privateKey.location", "", "auth.tlsKeyFile");

    public static final Property<String> PROPERTY_TLS_ENABLED_FOR_SEGMENT_STORE = Property.named(
            "segmentstore.connect.channel.tls", "", "auth.segmentStoreTlsEnabled");

    public static final Property<Integer> PROPERTY_SEGMENT_STORE_REQUEST_TIMEOUT_SECONDS = Property.named(
            "segmentstore.connect.channel.timeoutSeconds", 120, "");

    public static final Property<String> PROPERTY_ZK_URL = Property.named(
            "zk.connect.uri", "localhost:2181", "zkURL");

    public static final Property<Integer> PROPERTY_ZK_RETRY_MILLIS = Property.named(
            "zk.connect.retries.interval.milliseconds", 5000, "zk.retryIntervalMillis");

    public static final Property<Integer> PROPERTY_ZK_MAX_RETRY_COUNT = Property.named(
            "zk.connect.retries.count.max", 5, "maxRetries");

    public static final Property<Integer> PROPERTY_ZK_SESSION_TIMEOUT_MILLIS = Property.named(
            "zk.connect.session.timeout.milliseconds", 10000, "sessionTimeoutMillis");

    public static final Property<Boolean> PROPERTY_ZK_SECURE_CONNECTION = Property.named(
            "zk.connect.security.enable", false, "secureConnection");

    public static final Property<String> PROPERTY_ZK_TRUSTSTORE_FILE_PATH = Property.named(
            "zk.connect.security.tls.trustStore.location", "", "zk.tlsTrustStoreFile");

    public static final Property<String> PROPERTY_ZK_TRUSTSTORE_PASSWORD_FILE_PATH = Property.named(
            "zk.connect.security.tls.trustStore.pwd.location", "zk.tlsTrustStorePasswordFile");

    public static final Property<Integer> PROPERTY_RETENTION_FREQUENCY_MINUTES = Property.named(
            "retention.frequency.minutes", 30, "retention.frequencyMinutes");

    public static final Property<Integer> PROPERTY_RETENTION_BUCKET_COUNT = Property.named(
            "retention.bucket.count", 1, "retention.bucketCount");

    public static final Property<Integer> PROPERTY_RETENTION_THREAD_COUNT = Property.named(
            "retention.thread.count", 1, "retention.threadCount");

    public static final Property<Integer> PROPERTY_TXN_MIN_LEASE = Property.named(
            "transaction.lease.count.min", 10000, "transaction.minLeaseValue");

    public static final Property<Long> PROPERTY_TXN_MAX_LEASE = Property.named(
            "transaction.lease.count.max", 600000L, "transaction.maxLeaseValue");

    public static final Property<Integer> PROPERTY_TXN_MAX_EXECUTION_TIMEBOUND_DAYS = Property.named(
            "transaction.execution.timeBound.days", 1);

    public static final Property<Integer> PROPERTY_TXN_TTL_HOURS = Property.named(
            "transaction.ttl.hours", 24, "transaction.ttlHours");

    public static final Property<Integer> PROPERTY_WATERMARKING_FREQUENCY_SECONDS = Property.named(
            "watermarking.frequency.seconds", 10, "watermarking.frequencySeconds");

    public static final Property<Integer> PROPERTY_WATERMARKING_BUCKET_COUNT = Property.named(
            "watermarking.bucket.count", 100, "watermarking.bucketCount");

    public static final Property<Integer> PROPERTY_WATERMARKING_THREAD_COUNT = Property.named(
            "watermarking.thread.count", 10, "watermarking.threadCount");

    public static final Property<String> PROPERTY_SCALE_STREAM_NAME = Property.named(
            "scale.request.stream.name", "_requeststream", "scale.streamName");

    public static final Property<String> PROPERTY_SCALE_READER_GROUP = Property.named(
            "scale.request.readerGroup.name", "scaleGroup", "scale.ReaderGroup");

    public static final Property<Integer> PROPERTY_HEALTH_CHECK_FREQUENCY = Property.named(
            "health.frequency.seconds", 10);

    public static final Property<Integer> PROPERTY_LIST_COMPLETED_TXN_MAX_RECORDS = Property.named(
            "listCompletedTxn.max.records", 500);

    public static final Property<Integer> PROPERTY_MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS = Property.named(
            "min.bucket.redistribution.interval.in.seconds", 10);

    public static final String COMPONENT_CODE = "controller";

    //endregion

    //#region Properties

    //RPC Server configuration
    public static final int RPC_SERVER_PORT;
    public static final int ASYNC_TASK_POOL_SIZE;
    public static final String RPC_PUBLISHED_SERVER_HOST;
    public static final int RPC_PUBLISHED_SERVER_PORT;

    // Pravega Service endpoint configuration. Used only for a standalone single node deployment.
    public static final String SERVICE_HOST;
    public static final int SERVICE_PORT;

    //HostStore configuration.
    public static final int HOST_STORE_CONTAINER_COUNT;

    //Cluster configuration.
    public static final boolean HOST_MONITOR_ENABLED;
    public static final String CLUSTER_NAME;
    public static final int CLUSTER_MIN_REBALANCE_INTERVAL;

    // Security configuration
    public static final boolean AUTHORIZATION_ENABLED;
    public static final String USER_PASSWORD_FILE;
    public static final boolean TLS_ENABLED;
    public static final List<String> TLS_PROTOCOL_VERSION;
    public static final String TLS_KEY_FILE;
    public static final String TLS_CERT_FILE;
    public static final String TLS_TRUST_STORE;
    public static final String TOKEN_SIGNING_KEY;
    public static final int ACCESS_TOKEN_TTL_IN_SECONDS;
    public static final String TLS_ENABLED_FOR_SEGMENT_STORE;
    public static final boolean WRITES_TO_RGSTREAMS_WITH_READ_PERMISSIONS;

    public static final boolean REPLY_WITH_STACK_TRACE_ON_ERROR;
    public static final boolean REQUEST_TRACING_ENABLED;

    // Zookeeper configuration.
    public static final String ZK_URL;
    public static final int ZK_RETRY_SLEEP_MS;
    public static final int ZK_MAX_RETRIES;
    public static final int ZK_SESSION_TIMEOUT_MS;
    public static final boolean SECURE_ZK;
    public static final String ZK_TRUSTSTORE_FILE_PATH;
    public static final String ZK_TRUSTSTORE_PASSWORD_FILE_PATH;

    // REST server configuration
    public static final String REST_SERVER_IP;
    public static final int REST_SERVER_PORT;
    public static final String REST_KEYSTORE_FILE_PATH;
    public static final String REST_KEYSTORE_PASSWORD_FILE_PATH;

    // Store configuration
    public static final boolean USE_PRAVEGA_TABLES;
    //Transaction configuration
    public static final long MIN_LEASE_VALUE;
    public static final long MAX_LEASE_VALUE;
    public static final int MAX_TXN_EXECUTION_TIMEBOUND_DAYS;

    // Completed Transaction TTL
    public static final int COMPLETED_TRANSACTION_TTL_IN_HOURS;

    // Retention Configuration
    public static final int MINIMUM_RETENTION_FREQUENCY_IN_MINUTES;
    public static final int RETENTION_BUCKET_COUNT;
    public static final int RETENTION_THREAD_POOL_SIZE;

    // Watermarking Configuration
    public static final int MINIMUM_WATERMARKING_FREQUENCY_IN_SECONDS;
    public static final int WATERMARKING_BUCKET_COUNT;
    public static final int WATERMARKING_THREAD_POOL_SIZE;

    // Request Stream Configuration
    public static final String SCALE_STREAM_NAME;

    // Request Stream readerGroup
    public static final String SCALE_READER_GROUP;

    // Print stack trace for all threads during shutdown
    public static final boolean DUMP_STACK_ON_SHUTDOWN;

    public static final MetricsConfig METRICS_CONFIG;
    public static final GRPCServerConfig GRPC_SERVER_CONFIG;

    public static final Integer REQUEST_TIMEOUT_SECONDS_SEGMENT_STORE;

    public static final int HEALTH_CHECK_FREQUENCY;

    public static final int LIST_COMPLETED_TXN_MAX_RECORDS;

    public static final int MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS;

    private static final String METRICS_PATH = "controller.metrics.";

    //endregion


    //region Initialization

    static {
        val properties = loadConfiguration();
        val p = new TypedProperties(properties, COMPONENT_CODE);

        RPC_SERVER_PORT = p.getInt(PROPERTY_SERVICE_PORT);
        ASYNC_TASK_POOL_SIZE = p.getInt(PROPERTY_TASK_POOL_SIZE);
        RPC_PUBLISHED_SERVER_HOST = p.get(PROPERTY_RPC_HOST);
        RPC_PUBLISHED_SERVER_PORT = p.getInt(PROPERTY_RPC_PORT);
        SERVICE_HOST = p.get(PROPERTY_SERVICE_HOST_IP);
        SERVICE_PORT = p.getInt(PROPERTY_SERVICE_HOST_PORT);
        HOST_STORE_CONTAINER_COUNT = p.getInt(PROPERTY_CONTAINER_COUNT);
        HOST_MONITOR_ENABLED = p.getBoolean(PROPERTY_HOST_MONITORING_ENABLED);
        CLUSTER_NAME = p.get(PROPERTY_CLUSTER_NAME);
        CLUSTER_MIN_REBALANCE_INTERVAL = p.getInt(PROPERTY_MIN_REBALANCE_INTERVAL_SECONDS);

        AUTHORIZATION_ENABLED = p.getBoolean(PROPERTY_AUTH_ENABLED);
        USER_PASSWORD_FILE = p.get(PROPERTY_PWD_AUTH_HANDLER_ACCOUNTS_STORE);
        TOKEN_SIGNING_KEY = p.get(PROPERTY_TOKEN_SIGNING_KEY);
        ACCESS_TOKEN_TTL_IN_SECONDS = p.getInt(PROPERTY_ACCESS_TOKEN_TTL_SECONDS);
        WRITES_TO_RGSTREAMS_WITH_READ_PERMISSIONS = p.getBoolean(PROPERTY_WRITES_TO_RGSTREAMS_WITH_READ_PERMISSIONS);

        TLS_ENABLED = p.getBoolean(PROPERTY_TLS_ENABLED);
        String[] protocols = new TLSProtocolVersion(p.get(PROPERTY_TLS_PROTOCOL_VERSION)).getProtocols();
        TLS_PROTOCOL_VERSION = Collections.unmodifiableList(Arrays.asList(protocols));
        TLS_KEY_FILE = p.get(PROPERTY_TLS_KEY_FILE);
        TLS_CERT_FILE = p.get(PROPERTY_TLS_CERT_FILE);
        TLS_TRUST_STORE = p.get(PROPERTY_TLS_TRUST_STORE);
        TLS_ENABLED_FOR_SEGMENT_STORE = p.get(PROPERTY_TLS_ENABLED_FOR_SEGMENT_STORE);

        REPLY_WITH_STACK_TRACE_ON_ERROR = p.getBoolean(PROPERTY_REPLY_WITH_STACK_TRACE_ON_ERROR);
        REQUEST_TRACING_ENABLED = p.getBoolean(PROPERTY_REQUEST_TRACING_ENABLED);

        ZK_URL = p.get(PROPERTY_ZK_URL);
        ZK_RETRY_SLEEP_MS = p.getInt(PROPERTY_ZK_RETRY_MILLIS);
        ZK_MAX_RETRIES = p.getInt(PROPERTY_ZK_MAX_RETRY_COUNT);
        ZK_SESSION_TIMEOUT_MS = p.getInt(PROPERTY_ZK_SESSION_TIMEOUT_MILLIS);
        SECURE_ZK = p.getBoolean(PROPERTY_ZK_SECURE_CONNECTION);
        ZK_TRUSTSTORE_FILE_PATH = p.get(PROPERTY_ZK_TRUSTSTORE_FILE_PATH);
        ZK_TRUSTSTORE_PASSWORD_FILE_PATH = p.get(PROPERTY_ZK_TRUSTSTORE_PASSWORD_FILE_PATH);

        REST_SERVER_IP = p.get(PROPERTY_REST_IP);
        REST_SERVER_PORT = p.getInt(PROPERTY_REST_PORT);
        REST_KEYSTORE_FILE_PATH = p.get(PROPERTY_REST_KEYSTORE_FILE_PATH);
        REST_KEYSTORE_PASSWORD_FILE_PATH = p.get(PROPERTY_REST_KEYSTORE_PASSWORD_FILE_PATH);

        MIN_LEASE_VALUE = p.getInt(PROPERTY_TXN_MIN_LEASE);
        MAX_LEASE_VALUE = p.getLong(PROPERTY_TXN_MAX_LEASE);
        MAX_TXN_EXECUTION_TIMEBOUND_DAYS = p.getInt(PROPERTY_TXN_MAX_EXECUTION_TIMEBOUND_DAYS);
        COMPLETED_TRANSACTION_TTL_IN_HOURS = p.getInt(PROPERTY_TXN_TTL_HOURS);
        MINIMUM_RETENTION_FREQUENCY_IN_MINUTES = p.getInt(PROPERTY_RETENTION_FREQUENCY_MINUTES);
        RETENTION_BUCKET_COUNT = p.getInt(PROPERTY_RETENTION_BUCKET_COUNT);
        RETENTION_THREAD_POOL_SIZE = p.getInt(PROPERTY_RETENTION_THREAD_COUNT);
        MINIMUM_WATERMARKING_FREQUENCY_IN_SECONDS = p.getInt(PROPERTY_WATERMARKING_FREQUENCY_SECONDS);
        WATERMARKING_BUCKET_COUNT = p.getInt(PROPERTY_WATERMARKING_BUCKET_COUNT);
        WATERMARKING_THREAD_POOL_SIZE = p.getInt(PROPERTY_WATERMARKING_THREAD_COUNT);
        SCALE_STREAM_NAME = p.get(PROPERTY_SCALE_STREAM_NAME);
        SCALE_READER_GROUP = p.get(PROPERTY_SCALE_READER_GROUP);
        DUMP_STACK_ON_SHUTDOWN = p.getBoolean(PROPERTY_DUMP_STACK_ON_SHUTDOWN);
        USE_PRAVEGA_TABLES = p.getBoolean(PROPERTY_USE_PRAVEGA_TABLES);
        GRPC_SERVER_CONFIG = createGrpcServerConfig();
        METRICS_CONFIG = createMetricsConfig(properties);

        REQUEST_TIMEOUT_SECONDS_SEGMENT_STORE = p.getInt(PROPERTY_SEGMENT_STORE_REQUEST_TIMEOUT_SECONDS);
        HEALTH_CHECK_FREQUENCY = p.getInt(PROPERTY_HEALTH_CHECK_FREQUENCY);
        LIST_COMPLETED_TXN_MAX_RECORDS = p.getInt(PROPERTY_LIST_COMPLETED_TXN_MAX_RECORDS);

        MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS = p.getInt(PROPERTY_MIN_BUCKET_REDISTRIBUTION_INTERVAL_IN_SECONDS);
    }

    private static Properties loadConfiguration() {
        // Fetch configuration in a specific order (from lowest priority to highest).
        Properties properties = new Properties();
        properties.putAll(loadFromFile());
        properties.putAll(System.getenv());
        properties.putAll(System.getProperties());

        // Resolve references against the loaded properties.
        properties = resolveReferences(properties);

        log.info("Controller configuration:");
        properties.forEach((k, v) -> log.info("{} = {}", k, v));
        return properties;
    }

    @SneakyThrows(IOException.class)
    private static Properties loadFromFile() {
        Properties result = new Properties();

        File file = findConfigFile();
        if (file == null) {
            ClassLoader classLoader = Config.class.getClassLoader();
            URL url = classLoader.getResource("controller.config.properties");
            if (url != null) {
                file = new File(url.getFile());
                if (!file.exists()) {
                    file = null;
                }
            }
        }

        if (file != null) {
            try (FileReader reader = new FileReader(file)) {
                result.load(reader);
            }
            log.info("Loaded {} config properties from {}.", result.size(), file);
        }

        return result;
    }

    private static File findConfigFile() {
        File result = Arrays.stream(new String[]{"conf.file", "config.file"})
                            .map(System::getProperty)
                            .filter(s -> !Strings.isNullOrEmpty(s))
                            .map(File::new)
                            .filter(File::exists)
                            .findFirst()
                            .orElse(new File("controller.config.properties"));

        return result.exists() ? result : null;
    }

    private static Properties resolveReferences(Properties properties) {
        // Any value that looks like ${REF} will need to be replaced by the value of REF in source.
        final String pattern = "^\\$\\{(.+)\\}$";
        val resolved = new Properties();
        for (val e : properties.entrySet()) {
            // Fetch reference value.
            String existingValue = e.getValue().toString();
            String newValue = existingValue; // Default to existing value (in case it's not a reference).
            if (existingValue.matches(pattern)) {
                // Only include the referred value if it resolves to anything; otherwise exclude this altogether.
                String lookupKey = existingValue.replaceAll(pattern, "$1");
                newValue = (String) properties.getOrDefault(lookupKey, null);
                if (newValue != null) {
                    log.info("Config property '{}={}' resolved to '{}'.", e.getKey(), existingValue, newValue);
                }
            }

            if (newValue != null) {
                resolved.put(e.getKey().toString(), newValue);
            }
        }

        return resolved;
    }

    private static GRPCServerConfig createGrpcServerConfig() {
        String publishHost = Config.RPC_PUBLISHED_SERVER_HOST;
        if (publishHost != null && publishHost.equals(NULL_VALUE)) {
            publishHost = null;
            log.info("publishedRPCHost is not configured, will use default value");
        }

        return GRPCServerConfigImpl.builder()
                .port(Config.RPC_SERVER_PORT)
                .publishedRPCHost(publishHost)
                .publishedRPCPort(Config.RPC_PUBLISHED_SERVER_PORT)
                .authorizationEnabled(Config.AUTHORIZATION_ENABLED)
                .userPasswordFile(Config.USER_PASSWORD_FILE)
                .tlsEnabled(Config.TLS_ENABLED)
                .tlsProtocolVersion(Config.TLS_PROTOCOL_VERSION.toArray(new String[Config.TLS_PROTOCOL_VERSION.size()]))
                .tlsCertFile(Config.TLS_CERT_FILE)
                .tlsTrustStore(Config.TLS_TRUST_STORE)
                .tlsKeyFile(Config.TLS_KEY_FILE)
                .tokenSigningKey(Config.TOKEN_SIGNING_KEY)
                .accessTokenTTLInSeconds(Config.ACCESS_TOKEN_TTL_IN_SECONDS)
                .isRGWritesWithReadPermEnabled(Config.WRITES_TO_RGSTREAMS_WITH_READ_PERMISSIONS)
                .replyWithStackTraceOnError(Config.REPLY_WITH_STACK_TRACE_ON_ERROR)
                .requestTracingEnabled(Config.REQUEST_TRACING_ENABLED)
                .build();
    }

    private static MetricsConfig createMetricsConfig(Properties p) {
        val builder = MetricsConfig.builder();
        for (val e : p.entrySet()) {
            String key = (String) e.getKey();
            if (key.startsWith(METRICS_PATH)) {
                builder.with(Property.named(key.substring(METRICS_PATH.length())), e.getValue());
            }
        }

        return builder.build();
    }

    //endregion
}
