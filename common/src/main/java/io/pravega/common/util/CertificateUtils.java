/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.Exceptions;
import lombok.NonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;

public class CertificateUtils {

    /**
     * Retrieves certificates from PEM-encoded (ASCII) or DER-encoded (binary) file at the specified
     * {@code certFilePath}.
     *
     * @param certFilePath the path of the file containing a certificate(s).
     * @return a sequence of X509Certificate certificates found in the file.
     * @throws CertificateException if parsing of the certificate fails
     * @throws FileNotFoundException if a file in not found at the specified {@code certFilePath}
     * @throws NullPointerException if {@code certFilePath} is null
     * @throws IllegalArgumentException if {@code certFilePath} is empty
     */
    public static X509Certificate[] extractCerts(@NonNull String certFilePath)
            throws CertificateException, FileNotFoundException {
        Exceptions.checkNotNullOrEmpty(certFilePath, "certFilePath");

        Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(new FileInputStream(new File(certFilePath)));
        final X509Certificate[] result = new X509Certificate[certificates.size()];

        int i = 0;
        for (Certificate cert: certificates) {
            result[i++] = (X509Certificate) cert;
        }
        return result;
    }

    /**
     * Creates a truststore with certificates from PEM-encoded (ASCII) or DER-encoded (binary) file at
     * the specified {@code certFilePath}.
     *
     * @param certFilePath the path of the file containing a certificate chain.
     * @return a truststore loaded with the certificate chain present in the specified {@code certFilePath}
     * @throws CertificateException if parsing of the certificate fails
     * @throws IOException if a file in not found at the specified {@code certFilePath}, or some other IO error occurs
     * @throws KeyStoreException if creating an instance of the truststore fails for some reason.
     * @throws NoSuchAlgorithmException if the algorithm used for verifying rhe truststore's integrity is missing
     * @throws NullPointerException if {@code certFilePath} is null
     * @throws IllegalArgumentException if {@code certFilePath} is empty
     */
    public static KeyStore createTrustStore(@NonNull String certFilePath)
            throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        return createTrustStore(extractCerts(certFilePath));
    }

    private static KeyStore createTrustStore(X509Certificate[] certificateChain)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {

        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        int i = 0;
        for (X509Certificate cert: certificateChain) {
            String alias = i++ + "";
            trustStore.setCertificateEntry(alias, cert);
        }
        return trustStore;
    }

}
