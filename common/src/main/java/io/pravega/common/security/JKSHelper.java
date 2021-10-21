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
package io.pravega.common.security;

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
        File passwordFile = new File(zkTrustStorePasswordPath);
        if (passwordFile.length() == 0 || passwordFile.length() > MAX_FILE_LENGTH) {
            return "";
        }
        try {
            return FileUtils.readFileToString(passwordFile, Charsets.UTF_8).trim();
        } catch (IOException e) {
            log.warn("Exception while parsing the password file.", e);
            return "";
        }
    }

}
