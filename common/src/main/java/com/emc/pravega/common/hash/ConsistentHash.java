/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class ConsistentHash {

    static final String MD_5 = "MD5";
    private static MessageDigest md;

    {
        try {
            md = MessageDigest.getInstance(MD_5);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public static int hash(String str, int numOfPoints) {
            byte[] data = str.getBytes();
            md.update(data, 0, data.length);
            BigInteger i = new BigInteger(1, md.digest());
            return i.intValue() % numOfPoints;
    }
}
