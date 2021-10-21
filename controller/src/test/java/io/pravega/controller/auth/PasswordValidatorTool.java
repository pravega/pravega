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
package io.pravega.controller.auth;

import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

public class PasswordValidatorTool {

    public static void main(String[] args) throws InvalidKeySpecException, NoSuchAlgorithmException {
        String userName = args[0];
        String userPassword = args[1];
        String encryptedPassword = args[2];
        StrongPasswordProcessor passwordEncryptor = StrongPasswordProcessor.builder().build();
        System.out.println( passwordEncryptor.checkPassword(userPassword.toCharArray(), encryptedPassword));
    }
}
