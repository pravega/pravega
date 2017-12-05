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

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.security.cert.CertificateException;

public class JWTTester {
    public static void main(String[] args) throws CertificateException, FileNotFoundException, UnsupportedEncodingException {


      /*  RSAPublicKey publicKey = certificate.getPublicKey();
                //Get the key instance
                RSAPrivateKey privateKey = //Get the key instance
*/
        Jwts.builder()
            .setSubject("Joe")
            .signWith(SignatureAlgorithm.HS512, "secret")
            .compact();
    }
}
