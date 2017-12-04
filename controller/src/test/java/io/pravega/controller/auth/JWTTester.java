/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.auth;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.PublicKey;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import org.jasypt.util.password.StrongPasswordEncryptor;

public class JWTTester {
    public static void main(String[] args) throws CertificateException, FileNotFoundException {

        FileInputStream fin = new FileInputStream("config/cert.pem");
        CertificateFactory f = CertificateFactory.getInstance("X.509");
        X509Certificate certificate = (X509Certificate)f.generateCertificate(fin);
        RSAPublicKey publicKey = (RSAPublicKey) certificate.getPublicKey();
        fin = new FileInputStream("config/key.pem");
        certificate = (X509Certificate) f.generateCertificate(fin);
        RSAPrivateKey privateKey = (RSAPrivateKey) certificate.getPublicKey();
      /*  RSAPublicKey publicKey = certificate.getPublicKey();
                //Get the key instance
                RSAPrivateKey privateKey = //Get the key instance
*/
        String token = JWT.create()
                .withClaim("myclaim",false)
                          .withIssuer("controller")
                          .withAudience("segmentstore")
                          .sign( Algorithm.RSA256(publicKey, privateKey));
    }
}
