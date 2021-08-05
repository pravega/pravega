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

import java.util.Arrays;

public class TLSProtocolVersion {

    private final String[] protocols;

    public TLSProtocolVersion(String s) {
        protocols = parse(s);
    }

    public String[] getProtocols() {
        return Arrays.copyOf(protocols, protocols.length);
    }

    public static final String[] parse(String s) {
        String[] protocols = s.split(",");
        for (String a : protocols) {
            if (!a.matches("TLSv1\\.(2|3)")) {
                throw new IllegalArgumentException("Invalid TLS Protocol Version");
            }
        }
        return protocols;
    }
}