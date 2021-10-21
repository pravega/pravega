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

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Base64;

import io.pravega.test.common.AssertExtensions;
import lombok.SneakyThrows;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CertificateUtilsTests {

    private final static String TEST_CERT_PEM = "-----BEGIN CERTIFICATE-----" +
            "MIIDCTCCAfGgAwIBAgIUEiI4I+eInKkuvvdk99KSjjdashgwDQYJKoZIhvcNAQEL" +
            "BQAwFDESMBAGA1UEAwwJVGVzdC1DZXJ0MB4XDTIwMDUxMjE3MjEzMloXDTMwMDUx" +
            "MDE3MjEzMlowFDESMBAGA1UEAwwJVGVzdC1DZXJ0MIIBIjANBgkqhkiG9w0BAQEF" +
            "AAOCAQ8AMIIBCgKCAQEAo3jTcJf5F1f06UYL39bWetOgOqNbzTn6IVklNSEHrgB8" +
            "ymIt9zLTIw78ho/RIPKyjr4xkh29Rg3O6aOJCsjfjOE37QhNPuXifKagSJSrEHRs" +
            "0QzhZlM6ZuyxQq7kyjpg95HIxzkbTkNDXKpD+Q8UCGctGoFGNYpy9IZBYR6FiCaG" +
            "Vw0xTvpClwDcx5Mo1DtyDtgWEpLIPSHNu50Tzzq8XoDgD91VeTxL2zIbeu1TJT61" +
            "MhIzCVMqCEvQD1mjwk/T+s1N2f5z77HszhnqUBU72875JhcINjH6hqMB5sRRsGL0" +
            "bt27EJyVnzKIzAieFpGLSNojuoKppIhZgxFoS8HyEQIDAQABo1MwUTAdBgNVHQ4E" +
            "FgQUoEv5CbUn/LocHrN6TvPHYRqfYCkwHwYDVR0jBBgwFoAUoEv5CbUn/LocHrN6" +
            "TvPHYRqfYCkwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAQRvs" +
            "m6uZroAH0MMoEplyGiE9KlToc07FkPVh3wTC69uKBNhEnljN6FrmbU6M8sWHoCLN" +
            "Rl6eCETUzxemVcbd/wz2gP3B5da0SOHIcywO2mhvNfmiWZTuYT7puJC/YFA+freP" +
            "Y6uJCMXkRZUx/RuYXBs2iuVARdDki8mBite7Z9AhnyFxoGDYxYB7BiMNwNpoxChb" +
            "NRSACWn/g7B6TdCplkY40EFRFQx9uVJ/1sNwtcpq35fFA4WtWyJt9zPPvgFSBUZ7" +
            "EZZFfIelxL53LJXcH1j5bCJxmM0HSLeHgat10csDqQoWvApirxxrkQkhCPrgK0BC" +
            "s9IUX+HWjS8z1gzcmQ==" +
            "-----END CERTIFICATE-----";

    @Test
    public void testExtractCerts() {
        X509Certificate[] certs = extractCerts();
        assertNotNull(certs);
        assertEquals("CN=Test-Cert", certs[0].getSubjectX500Principal().getName());
    }

    @SneakyThrows
    @Test
    public void testExtractCertsThrowsExceptionWhenFilePathIsBlank() {
        AssertExtensions.assertThrows("Expected exception was not thrown",
                () -> CertificateUtils.extractCerts(buildCertPath(null)), e -> e instanceof NullPointerException);
        AssertExtensions.assertThrows("Expected exception was not thrown",
                () -> CertificateUtils.extractCerts(buildCertPath("")), e -> e instanceof IllegalArgumentException);
    }

    @SneakyThrows
    @Test
    public void testCreateTruststoreThrowsExceptionWhenFilePathIsBlank() {
        AssertExtensions.assertThrows("Expected exception was not thrown",
                () -> CertificateUtils.createTrustStore(buildCertPath(null)), e -> e instanceof NullPointerException);
        AssertExtensions.assertThrows("Expected exception was not thrown",
                () -> CertificateUtils.createTrustStore(buildCertPath("")), e -> e instanceof IllegalArgumentException);
    }

    private String buildCertPath(String path) {
        return path;
    }

    @SneakyThrows
    @Test
    public void testCreateTrustStore() {
        X509Certificate[] certs = extractCerts();
        KeyStore trustStore = CertificateUtils.createTrustStore(certs);
        assertNotNull(trustStore);
        assertTrue(trustStore.containsAlias("0"));
    }

    @Test
    public void testToString() {
        X509Certificate[] certs = extractCerts();
        String certsAsString = CertificateUtils.toString(certs);
        assertTrue(certsAsString.contains("type=[X.509]"));
        assertTrue(certsAsString.contains("subject=[CN=Test-Cert]"));
    }

    @SneakyThrows
    private X509Certificate[] extractCerts() {
        String certBody = TEST_CERT_PEM.replaceAll("-----BEGIN CERTIFICATE-----", "")
                .replaceAll("-----END CERTIFICATE-----", "");
        byte[] certBytes = Base64.getDecoder().decode(certBody);
        try (ByteArrayInputStream is = new ByteArrayInputStream(certBytes)) {
            X509Certificate[] certs = CertificateUtils.extractCerts(is);
            return certs;
        }
    }
}
