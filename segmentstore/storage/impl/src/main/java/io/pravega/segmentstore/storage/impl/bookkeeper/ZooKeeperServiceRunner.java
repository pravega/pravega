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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.security.JKSHelper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import io.pravega.common.security.ZKTLSUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Helps run ZooKeeper Server in process.
 */
@RequiredArgsConstructor
@Slf4j
public class ZooKeeperServiceRunner implements AutoCloseable {
    public static final String PROPERTY_ZK_PORT = "zkPort";
    private static final String PROPERTY_SECURE_ZK = "secureZK";
    private static final String PROPERTY_ZK_KEY_STORE = "zkKeyStore";
    private static final String PROPERTY_ZK_KEY_STORE_PASSWORD_PATH = "zkKeyStorePasswordPath";
    private static final String PROPERTY_ZK_TRUST_STORE = "zkTrustStore";

    private static final String LOOPBACK_ADDRESS = "localhost";
    private final AtomicReference<ZooKeeperServer> server = new AtomicReference<>();
    private final AtomicReference<ServerCnxnFactory> serverFactory = new AtomicReference<ServerCnxnFactory>();
    private final int zkPort;
    private final boolean secureZK;
    private final String keyStore;
    private final String keyStorePasswordPath;
    private final String trustStore;
    private final AtomicReference<File> tmpDir = new AtomicReference<>();

    @Override
    public void close() throws Exception {
        stop();

        File t = this.tmpDir.getAndSet(null);
        if (t != null) {
            log.info("Cleaning up " + t);
            FileUtils.deleteDirectory(t);
        }
    }

    public void initialize() throws IOException {
        System.setProperty("zookeeper.4lw.commands.whitelist", "*"); // As of ZooKeeper 3.5 this is needed to not break start()
        if (this.tmpDir.compareAndSet(null, IOUtils.createTempDir("zookeeper", "inproc"))) {
            this.tmpDir.get().deleteOnExit();
        }
        if (secureZK) {
            ZKTLSUtils.setSecureZKServerProperties(this.keyStore, this.keyStorePasswordPath, this.trustStore, this.keyStorePasswordPath);
        }
    }

    /**
     * Starts the ZooKeeper Service in process.
     *
     * @throws Exception If an exception occurred.
     */
    public void start() throws Exception {
        Preconditions.checkState(this.tmpDir.get() != null, "Not Initialized.");
        val s = new ZooKeeperServer(this.tmpDir.get(), this.tmpDir.get(), ZooKeeperServer.DEFAULT_TICK_TIME);
        if (!this.server.compareAndSet(null, s)) {
            s.shutdown();
            throw new IllegalStateException("Already started.");
        }
        this.serverFactory.set(NettyServerCnxnFactory.createFactory());
        val address = LOOPBACK_ADDRESS + ":" + this.zkPort;
        log.info("Starting Zookeeper server at " + address + " ...");
        this.serverFactory.get().configure(new InetSocketAddress(LOOPBACK_ADDRESS, this.zkPort), 1000, 1000, secureZK);
        this.serverFactory.get().startup(s);

        if (!waitForServerUp(this.zkPort, this.secureZK, this.keyStore, this.keyStorePasswordPath, this.trustStore,
                this.keyStorePasswordPath)) {
            throw new IllegalStateException("ZooKeeper server failed to start");
        }
    }

    public void stop() {
        try {
            ServerCnxnFactory sf = this.serverFactory.getAndSet(null);
            if (sf != null) {
                sf.closeAll(ServerCnxn.DisconnectReason.CLOSE_ALL_CONNECTIONS_FORCED);
                sf.shutdown();
            }
        } catch (Throwable e) {
            log.warn("Unable to cleanly shutdown ZooKeeper connection factory", e);
        }

        try {
            ZooKeeperServer zs = this.server.getAndSet(null);
            if (zs != null) {
                zs.shutdown();
                ZKDatabase zkDb = zs.getZKDatabase();
                if (zkDb != null) {
                    // make ZK server close its log files
                    zkDb.close();
                }
            }

        } catch (Throwable e) {
            log.warn("Unable to cleanly shutdown ZooKeeper server", e);
        }

        if (secureZK) {
            ZKTLSUtils.unsetSecureZKServerProperties();
        }
    }

    /**
     * Blocks the current thread and awaits ZooKeeper to start running locally on the given port.
     *
     * @param zkPort The ZooKeeper Port.
     * @param secureZk Flag to notify whether the ZK is secure.
     * @param trustStore Location of the trust store.
     * @param keyStore Location of the key store.
     * @param keyStorePasswordPath Location of password path for key store.
     *                             Empty string if `secureZk` is false or a password does not exist.
     * @param trustStore Location of the trust store.
     * @param trustStorePasswordPath Location of password path for trust store.
     *                               Empty string if `secureZk` is false or a password does not exist.
     * @return True if ZooKeeper started within a specified timeout, false otherwise.
     */
    private static boolean waitForServerUp(int zkPort, boolean secureZk, String keyStore, String keyStorePasswordPath,
                                          String trustStore, String trustStorePasswordPath) {
        val address = LOOPBACK_ADDRESS + ":" + zkPort;
        if (secureZk) {
            return waitForSSLServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT, keyStore, keyStorePasswordPath,
                    trustStore, trustStorePasswordPath);
        } else {
            return LocalBookKeeper.waitForServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT);
        }
    }

    public static boolean waitForServerUp(int zkPort) {
        return waitForServerUp(zkPort, false, "", "", "", "");
    }

    private static boolean waitForSSLServerUp(String address, long timeout, String keyStore, String keyStorePasswdPath,
                                              String trustStore, String trustStorePasswordPath) {
        TimeoutTimer timeoutTimer = new TimeoutTimer(Duration.ofMillis(timeout));
        String[] split = address.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);

        while (true) {
            try {
                SSLContext context = SSLContext.getInstance("TLS");
                TrustManagerFactory trustManager = getTrustManager(trustStore, trustStorePasswordPath);
                KeyManagerFactory keyFactory = getKeyManager(keyStore, keyStorePasswdPath);
                context.init(keyFactory.getKeyManagers(), trustManager.getTrustManagers(), null);

                try (Socket sock = context.getSocketFactory().createSocket(new Socket(host, port), host, port, true);
                     OutputStream outstream = sock.getOutputStream()) {
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        log.info("Server UP");
                        return true;
                    }
                }
            } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException
                    | KeyManagementException | UnrecoverableKeyException e) {
                // ignore as this is expected
                log.warn("server  {} not up.", address,  e);
            }

            if (!timeoutTimer.hasRemaining()) {
                break;
            }
            Exceptions.handleInterrupted(() -> Thread.sleep(250));
        }
        return false;
    }

    private static TrustManagerFactory getTrustManager(String trustStore, String trustStorePasswordPath)
            throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        try (FileInputStream myKeys = new FileInputStream(trustStore)) {

            KeyStore myTrustStore = KeyStore.getInstance("JKS");
            myTrustStore.load(myKeys, JKSHelper.loadPasswordFrom(trustStorePasswordPath).toCharArray());
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(myTrustStore);
            return tmf;
        }
    }

    private static KeyManagerFactory getKeyManager(String keyStore, String keyStorePasswordPath)
            throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyManagerFactory kmf = null;

        try (FileInputStream myKeys = new FileInputStream(keyStore)) {
            KeyStore myKeyStore = KeyStore.getInstance("JKS");
            myKeyStore.load(myKeys, JKSHelper.loadPasswordFrom(keyStorePasswordPath).toCharArray());
            kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(myKeyStore, JKSHelper.loadPasswordFrom(keyStorePasswordPath).toCharArray());

            return kmf;
        }
    }

    /**
     * Main method that can be used to start ZooKeeper out-of-process using BookKeeperServiceRunner.
     * This is used when invoking this class via ProcessStarter.
     *
     * @param args Args.
     * @throws Exception If an error occurred.
     */
    public static void main(String[] args) throws Exception {
        int zkPort;
        boolean secureZK;
        String zkKeyStore;
        String zkKeyStorePasswdPath;
        String zkTrustStore;
        try {
            zkPort = Integer.parseInt(System.getProperty(PROPERTY_ZK_PORT));
            secureZK = Boolean.parseBoolean(System.getProperty(PROPERTY_SECURE_ZK, "false"));
            zkKeyStore = System.getProperty(PROPERTY_ZK_KEY_STORE);
            zkKeyStorePasswdPath = System.getProperty(PROPERTY_ZK_KEY_STORE_PASSWORD_PATH);
            zkTrustStore = System.getProperty(PROPERTY_ZK_TRUST_STORE);
        } catch (Exception ex) {
            System.out.println(String.format("Invalid or missing arguments (via system properties). Expected: %s(int). (%s)",
                    PROPERTY_ZK_PORT, ex.getMessage()));
            System.exit(-1);
            return;
        }

        ZooKeeperServiceRunner runner = new ZooKeeperServiceRunner(zkPort, secureZK,
                zkKeyStore, zkKeyStorePasswdPath, zkTrustStore);
        runner.initialize();
        runner.start();
        Thread.sleep(Long.MAX_VALUE);
    }
}
