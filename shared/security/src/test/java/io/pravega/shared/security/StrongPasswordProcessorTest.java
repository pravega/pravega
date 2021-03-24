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
package io.pravega.shared.security;

import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import io.pravega.shared.security.crypto.StrongPasswordProcessor;
import org.junit.Assert;
import org.junit.Test;

public class StrongPasswordProcessorTest {

    @Test
    public void checkPassword() throws InvalidKeySpecException, NoSuchAlgorithmException {
        StrongPasswordProcessor processor = StrongPasswordProcessor.builder().iterations(5000).build();
        String encrypted = processor.encryptPassword("1111_aaaa");
        StrongPasswordProcessor secondProcessor = StrongPasswordProcessor.builder().iterations(5000).build();

        secondProcessor.checkPassword("1111_aaaa".toCharArray(), encrypted);

        StrongPasswordProcessor failingProcessor = StrongPasswordProcessor.builder().iterations(1000).build();
        Assert.assertTrue("Passwords with different iterations should not match",
                !encrypted.equals(failingProcessor.encryptPassword("1111_aaaa")));
    }
}