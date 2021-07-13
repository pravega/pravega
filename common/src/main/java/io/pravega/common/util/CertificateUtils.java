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
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import lombok.NonNull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
     * @throws IOException if a file in not found at the specified {@code certFilePath} or some other IO error occurs.
     * @throws NullPointerException if {@code certFilePath} is null
     * @throws IllegalArgumentException if {@code certFilePath} is empty
     */
    public static X509Certificate[] extractCerts(String certFilePath)
            throws CertificateException, IOException {
        Exceptions.checkNotNullOrEmpty(certFilePath, "certFilePath");

        try (FileInputStream is = new FileInputStream(new File(certFilePath))) {
            return extractCerts(is);
        }
    }

    @VisibleForTesting
    static X509Certificate[] extractCerts(@NonNull InputStream certificateInputStream) throws CertificateException {
        Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(certificateInputStream);
        final X509Certificate[] result = new X509Certificate[certificates.size()];

        int i = 0;
        for (Certificate cert: certificates) {
            result[i++] = (X509Certificate) cert;
        }
        return result;
    }

    public static String toString(X509Certificate[] certificateChain) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < certificateChain.length; i++) {
            X509Certificate certificate = certificateChain[i];
            builder.append(" -> {");
            builder.append("type=[" + certificate.getType() + "], ");
            builder.append("subject=[" + certificate.getSubjectX500Principal() + "], ");
            builder.append("issuer=[" +
                    ((certificate.getIssuerDN() != null ? certificate.getIssuerDN().getName() : "None")) + "]");
            builder.append("}");
        }
        return builder.toString();
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
    public static KeyStore createTrustStore(String certFilePath)
            throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
        Exceptions.checkNotNullOrEmpty(certFilePath, "certFilePath");
        return createTrustStore(extractCerts(certFilePath));
    }

    @VisibleForTesting
    static KeyStore createTrustStore(X509Certificate[] certificateChain)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {

        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null, null);

        int i = 0;
        for (X509Certificate cert: certificateChain) {
            String alias = String.valueOf(i++);
            trustStore.setCertificateEntry(alias, cert);
        }
        return trustStore;
    }

}
