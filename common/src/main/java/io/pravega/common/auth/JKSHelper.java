/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.auth;

import com.google.common.base.Charsets;
import java.io.File;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

/**
 * Helper methods for handling JKS and JKS password files.
 * Please see here for further reference: https://docs.oracle.com/cd/E19509-01/820-3503/6nf1il6ek/index.html
 */
@Slf4j
public class JKSHelper {

    //Maximum length of the password file.
    private static final long MAX_FILE_LENGTH = 4 * 1024 * 1024;

    public static String loadPasswordFrom(String zkTrustStorePasswordPath) {
        byte[] password;
        File passwordFile = new File(zkTrustStorePasswordPath);
        if (passwordFile.length() == 0 || passwordFile.length() > MAX_FILE_LENGTH) {
            return "";
        }
        try {
            password = FileUtils.readFileToByteArray(passwordFile);
        } catch (IOException e) {
            log.warn("Exception while parsing the password file.", e);
            return "";
        }
        return new String(password, Charsets.UTF_8).trim();
    }

}
