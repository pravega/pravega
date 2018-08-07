/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import com.google.common.base.Preconditions;
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
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.NettyServerCnxnFactory;
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
    private static final String PROPERTY_ZK_KEY_STORE_PASSWD = "zkKeyStorePasswd";
    private static final String PROPERTY_ZK_TRUST_STORE = "zkTrustStore";

    //  private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private final AtomicReference<ZooKeeperServer> server = new AtomicReference<>();
    private final AtomicReference<ServerCnxnFactory> serverFactory = new AtomicReference<ServerCnxnFactory>();
    private final int zkPort;
    private final boolean secureZK;
    private final String keyStore;
    private final String keyStorePasswd;
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
            //-Dzookeeper.serverCnxnFactory=org.apache.zookeeper.server.NettyServerCnxnFactory
            //-Dzookeeper.ssl.keyStore.location=/root/zookeeper/ssl/testKeyStore.jks
            //-Dzookeeper.ssl.keyStore.password=testpass
            //-Dzookeeper.ssl.trustStore.location=/root/zookeeper/ssl/testTrustStore.jks
            //-Dzookeeper.ssl.trustStore.password=testpass
            System.setProperty("zookeeper.serverCnxnFactory", "org.apache.zookeeper.server.NettyServerCnxnFactory");

            System.setProperty("zookeeper.ssl.keyStore.location", this.keyStore);
            //TODO: Read these from the config/parameter files..
            System.setProperty("zookeeper.ssl.keyStore.password", loadPasswdFromFile(this.keyStorePasswd));
            System.setProperty("zookeeper.ssl.trustStore.location", "../config/bookie.truststore.jks");
            System.setProperty("zookeeper.ssl.trustStore.password", "1111_aaaa");
        }
    }

    private String loadPasswdFromFile(String keyStorePasswd) {
        byte[] pwd;
        File passwdFile = new File(keyStorePasswd);
        if (passwdFile.length() == 0) {
            return "";
        }
        try {
            pwd = FileUtils.readFileToByteArray(passwdFile);
        } catch (IOException e) {
            return "";
        }
        return new String(pwd).trim();
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

        if (!this.secureZK) {
            this.serverFactory.set(NIOServerCnxnFactory.createFactory());
        } else {
            this.serverFactory.set(NettyServerCnxnFactory.createFactory());
        }
        val address = "localhost:" + this.zkPort;
        log.info("Starting Zookeeper server at " + address + " ...");
        this.serverFactory.get().configure(new InetSocketAddress("localhost", this.zkPort), 1000, secureZK);
        this.serverFactory.get().startup(s);

        if (!waitForServerUp(this.zkPort, this.secureZK, this.trustStore, this.keyStore)) {
            throw new IllegalStateException("ZooKeeper server failed to start");
        }
    }

    public void stop() {
        try {
            ServerCnxnFactory sf = this.serverFactory.getAndSet(null);
            if (sf != null) {
                sf.closeAll();
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
            System.clearProperty("zookeeper.serverCnxnFactory");
            System.clearProperty("zookeeper.ssl.keyStore.location");
            System.clearProperty("zookeeper.ssl.keyStore.password");
            System.clearProperty("zookeeper.ssl.trustStore.location");
            System.clearProperty("zookeeper.ssl.trustStore.password");
        }
    }

    /**
     * Blocks the current thread and awaits ZooKeeper to start running locally on the given port.
     *
     * @param zkPort The ZooKeeper Port.
     * @param secureZk Flag to notify whether the ZK is secure.
     * @param trustStore Location of the trust store.
     * @param keyStore Location of the key store.
     * @return True if ZooKeeper started within a specified timeout, false otherwise.
     */
    public static boolean waitForServerUp(int zkPort, boolean secureZk, String trustStore, String keyStore) {
        val address = "localhost:" + zkPort;
        if (secureZk) {
            return waitForSSLServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT, trustStore, keyStore);
        } else {
            return LocalBookKeeper.waitForServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT);
        }
    }

    private static boolean waitForSSLServerUp(String address, long timeout, String trustStore, String keyStore) {
        //TODO: Create a ZK client to ensure that the server is up.
        long start = MathUtils.now();
        String[] split = address.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);

        while (true) {
            try {
                SSLContext context = SSLContext.getInstance("TLS");
                TrustManagerFactory trustManager = getTrustManager(trustStore);
                KeyManagerFactory keyFactory = getKeyManager(keyStore);

                context.init(keyFactory.getKeyManagers(), trustManager.getTrustManagers(), null);
                SSLSocketFactory factory = context.getSocketFactory();

                Socket sock = context.getSocketFactory().createSocket(new Socket(host, port), host, port, true);
                BufferedReader reader = null;
                try {
                    OutputStream outstream = sock.getOutputStream();
                    outstream.write("stat".getBytes());
                    outstream.flush();

                    reader =
                            new BufferedReader(
                                    new InputStreamReader(sock.getInputStream()));
                    String line = reader.readLine();
                    if (line != null && line.startsWith("Zookeeper version:")) {
                        log.info("Server UP");
                        return true;
                    }
                } catch (Exception e) {
                  e.printStackTrace();
                } finally {
                    sock.close();
                    if (reader != null) {
                        reader.close();
                    }
                }
            } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException | UnrecoverableKeyException e) {
                // ignore as this is expected
                log.info("server " + address + " not up " + e);
            }

            if (MathUtils.now() > start + timeout) {
                break;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return false;
    }

    private static TrustManagerFactory getTrustManager(String trustStore) throws IOException, CertificateException, NoSuchAlgorithmException, KeyStoreException {
        try (FileInputStream myKeys = new FileInputStream(trustStore)) {

            // Do the same with your trust store this time
            // Adapt how you load the keystore to your needs
            KeyStore myTrustStore = KeyStore.getInstance("JKS");
            myTrustStore.load(myKeys, "1111_aaaa".toCharArray());

            myKeys.close();

            TrustManagerFactory tmf = TrustManagerFactory
                    .getInstance("SunX509");
            tmf.init(myTrustStore);
            return tmf;
        }
    }

    private static KeyManagerFactory getKeyManager(String keyStore) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException {
        KeyManagerFactory kmf = null;

        try (FileInputStream myKeys = new FileInputStream(keyStore)) {
            KeyStore myKeyStore = KeyStore.getInstance("JKS");
             myKeyStore.load(myKeys, "1111_aaaa".toCharArray());

            myKeys.close();
            kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(myKeyStore, "1111_aaaa".toCharArray());

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
        boolean secureZK = false;
        String zkKeyStore;
        String zkKeyStorePasswd = null;
        String zkTrustStore = null;
        try {
            zkPort = Integer.parseInt(System.getProperty(PROPERTY_ZK_PORT));
            secureZK = Boolean.parseBoolean(System.getProperty(PROPERTY_SECURE_ZK, "false"));
            zkKeyStore = System.getProperty(PROPERTY_ZK_KEY_STORE);
            zkKeyStorePasswd = System.getProperty(PROPERTY_ZK_KEY_STORE_PASSWD);
            zkTrustStore = System.getProperty(PROPERTY_ZK_TRUST_STORE);
        } catch (Exception ex) {
            System.out.println(String.format("Invalid or missing arguments (via system properties). Expected: %s(int). (%s)",
                    PROPERTY_ZK_PORT, ex.getMessage()));
            System.exit(-1);
            return;
        }

        ZooKeeperServiceRunner runner = new ZooKeeperServiceRunner(zkPort, secureZK,
                zkKeyStore, zkKeyStorePasswd, zkTrustStore);
        runner.initialize();
        runner.start();
        Thread.sleep(Long.MAX_VALUE);
    }
}
